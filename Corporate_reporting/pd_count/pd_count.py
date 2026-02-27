import gzip
import json
import sys
import requests
import os
import io
import subprocess
from collections import defaultdict
from datetime import *
import pandas as pd
from urllib.parse import urlparse


class Proactive_disclosure:
    def __init__(self, current_date):
        self.current_date = current_date
        self.pd_list = ["transition", "transition_deputy", "parliament_report",
                        "parliament_committee", "parliament_committee_deputy"]
        self.headers = ["date"]
        self.unstruct_pd_count = [self.current_date]
        self.df_pd_org = pd.DataFrame()
        self.df_melt = pd.DataFrame()
        self.df_unpd_org = pd.DataFrame()
        self.record_added = False
        self.session = requests.Session()

    def _print_memory(self):
        print("[pd_count] Memory snapshot (free -h):")
        mem = subprocess.run(["free", "-h"], capture_output=True, text=True)
        print(mem.stdout.strip())

    def _print_git_status(self):
        print("[pd_count] Git status after file write:")
        status = subprocess.run(["git", "status", "--short"], capture_output=True, text=True)
        print(status.stdout.strip() if status.stdout.strip() else "(clean)")

    def _log_download(self, label):
        print(f"[pd_count] Downloaded: {label}")
        self._print_memory()

    def _log_file_write(self, path):
        print(f"[pd_count] Wrote file: {path}")
        self._print_git_status()

    def download(self):
        url = "https://open.canada.ca/static/od-do-canada.jsonl.gz"
        records = []
        try:
            with self.session.get(url, stream=True, timeout=(10, 120)) as response:
                response.raise_for_status()
                self._log_download(url)
                response.raw.decode_content = False
                with gzip.GzipFile(fileobj=response.raw) as fd:
                    for line in fd:
                        records.append(json.loads(line.decode('utf-8')))
                        if len(records) >= 500:
                            yield records
                            records = []
            if records:
                yield records
        except GeneratorExit:
            pass
        except Exception:
            import traceback
            traceback.print_exc()
            print('error reading downloaded file')
            sys.exit(0)

    # Structured PDs download
    def filedow(self, reqURL):
        try:
            req = self.session.get(reqURL, timeout=(10, 120))
            req.raise_for_status()
            filename = os.path.basename(urlparse(reqURL).path)
            filename = filename.split(".")[0].strip()
            filename = "_".join(filename.split("-"))
            self.headers.append(filename)
            self._log_download(reqURL)
            df = pd.read_csv(io.StringIO(req.content.decode("utf-8")), low_memory=False)
            if filename != "adminaircraft":
                df_agg = df.groupby("owner_org").size().reset_index(name="count")
                df_agg["type"] = filename
                self.df_pd_org = pd.concat([self.df_pd_org, df_agg], ignore_index=True)

            return filename, len(df)
        except Exception as e:
            print(e)

    # Create CSV file if it does not exist (initialize)
    def csv_file_create(self, csv_file, col_head):
        if not os.path.isfile(csv_file):
            print("no file should create")
            df = pd.DataFrame(columns=col_head)
            df.to_csv(csv_file, index=False, encoding="utf-8")
            self._log_file_write(csv_file)

    def add_record(self, row, csv_file, col_head, combined=False):
        self.csv_file_create(csv_file, col_head)
        df = pd.read_csv(csv_file)
        if row[0] in df['date'].values:
            print('record exist no overwriting ')
            self.record_added = False
            return
        else:
            df.loc[len(df.index)] = row
            df.sort_values(by='date', axis=0, ascending=False, inplace=True)
            df.reset_index(drop=True, inplace=True)
        if not combined:
            header = [col for col in col_head if col != "date"]
            df_unpivot = pd.melt(df, id_vars=['date'], value_vars=header)
            self.df_melt = pd.concat(
                [self.df_melt, df_unpivot], ignore_index=True)
        df.to_csv(csv_file, index=False)
        self._log_file_write(csv_file)
        self.record_added = True
        return

    def unstruct_pd(self):  # Non Structured pd types downloads and count
        total = 0
        non_structured_counts = defaultdict(int)
        unstructured_org_counts = defaultdict(lambda: defaultdict(int))
        pd_col = ["date"]
        pd_col.extend(self.pd_list)

        for records in self.download():
            for record in records:
                try:
                    collection = record["collection"]
                    if collection not in self.pd_list:
                        continue
                    owner_org = record["organization"]["name"]
                    non_structured_counts[collection] += 1
                    unstructured_org_counts[owner_org][collection] += 1
                except IndexError as e:
                    print(e)
                except Exception as e:
                    print(e)

        unpd_org_rows = []
        for elmt in self.pd_list:
            current_count = non_structured_counts[elmt]
            self.unstruct_pd_count.append(current_count)
            total += current_count

            for owner_org, counts in unstructured_org_counts.items():
                count = counts.get(elmt, 0)
                if count:
                    unpd_org_rows.append({"owner_org": owner_org, "count": count, "type": elmt})

        if unpd_org_rows:
            self.df_unpd_org = pd.DataFrame(unpd_org_rows)

        pd_col.extend(["total"])
        self.unstruct_pd_count.append(total)
        self.add_record(self.unstruct_pd_count, os.path.join("Corporate_reporting", "pd_count", "nonstruc_pd.csv"), pd_col)
        return total

    def struct_pd(self):
        pd_name = []
        row = [self.current_date]
        total = 0
        with open(os.path.join("Corporate_reporting", "pd_count", "links.txt"), "r") as f:
            Urls = [line.rstrip('\n') for line in f]
        f.close
        for url in Urls:
            filename, len_df = self.filedow(url)
            if filename != "adminaircraft":
                pd_name.append(filename)
            total += len_df
            row.append(len_df)
        pd_name.sort()
        self.headers.append("total")
        row.append(total)
        self.add_record(row, os.path.join("Corporate_reporting", "pd_count", "structure_pd.csv"), self.headers)
        return total

    def pd_combined(self):
        all_pd = [self.current_date]
        struc_pd_total = self.struct_pd()  # Structured pds total
        unstruc_pd_total = self.unstruct_pd()  # unstructured pds total
        total_all = struc_pd_total + unstruc_pd_total
        all_pd.extend([struc_pd_total, unstruc_pd_total, total_all])
        if self.record_added:
            self.df_melt.sort_values(
                by='date', axis=0, ascending=False, inplace=True)
            self.df_melt.rename(columns={"variable": "pd_type", "value": "pd_count"}, inplace=True)
            print(self.df_melt.head())
            self.df_melt = self.df_melt.query(f'pd_type != "total"')
            unpivoted_path = os.path.join("Corporate_reporting", "pd_count", "unpivoted_pd.csv")
            self.df_melt.to_csv(unpivoted_path, encoding="utf-8", index=False)
            self._log_file_write(unpivoted_path)
        self.add_record(all_pd, os.path.join("Corporate_reporting", "pd_count", "all_pd.csv"), [
            "date", "structured_pd", "non_structured_pd", "total"], True)

    def pd_per_dept(self):
        all_pd_df = pd.concat(
                        [self.df_unpd_org, self.df_pd_org], ignore_index=True)
        df_dpt = all_pd_df.pivot(index="owner_org", columns="type")
        df_dpt = df_dpt['count'].reset_index()
        df_dpt.columns.name = None
        df_dpt.fillna(0, inplace=True)
        df_dpt = df_dpt.astype({'transition': 'int', 'transition_deputy': 'int', 'parliament_report': 'int',
                        'parliament_committee': 'int', 'parliament_committee_deputy': 'int', 'ati_all': 'int', 'ati_nil': 'int', 'briefingt': 'int', 'contracts': 'int', 'contracts_nil': 'int', 'contractsa': 'int', 'dac': 'int', 'grants': 'int', 'grants_nil': 'int',
                                'hospitalityq': 'int', 'hospitalityq_nil': 'int', 'qpnotes': 'int', 'qpnotes_nil': 'int', 'reclassification': 'int', 'reclassification_nil': 'int', 'travela': 'int', 'travelq': 'int', 'travelq_nil': 'int', 'wrongdoing': 'int'})
        per_dept_path = os.path.join("Corporate_reporting", "pd_count", "pd_per_dept.csv")
        df_dpt.to_csv(per_dept_path, encoding='utf-8', index=False)
        self._log_file_write(per_dept_path)


def main():
    current_date = date.today().strftime('%Y-%m-%d')
    pd_obj = Proactive_disclosure(current_date)
    pd_obj.pd_combined()
    pd_obj.pd_per_dept()


if __name__ == '__main__':
    main()
