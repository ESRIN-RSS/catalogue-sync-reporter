"""This small program will query G-POD and COPHUB on the same datasets, in
order to obtain the number of data results, compile a table and email it.
"""
import logging
import re, os
import time
from datetime import datetime, timedelta
import shutil
import argparse
import calendar
import requests
import zipfile
import uuid

from send_email import send_from_gmail


def setup_cmd_args():
    """Setup command line arguments."""
    parser = argparse.ArgumentParser(description="This program will query G-POD and COPHUB on the same datasets, in order to obtain the number of data results, compare them compile a report with the differences.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # parser.add_argument("root_dir", help="The root directory containing data to check")
    # parser.add_argument("--workspace", help="Set Workspace manually")
    parser.add_argument("--outputlist", help="Folder to write the output lists with the un-synced products.", default="c:\\temp\\")
    parser.add_argument("--daysback", help="Report with a given number of days back from today", default=0)
    parser.add_argument("--dataset", help="Set which dataset to query (chose S3A_SR_1_SRA_A_PREOPS or S3B_SR_1_SRA_A_NTC)")
    parser.add_argument("--startdate", help=" The Start Date (format: YYYY-MM-DD) ", default="2016-06-01")
    parser.add_argument("--enddate",help=" The End Date (format: YYYY-MM-DD)")
    parser.add_argument("--cphubuser",help="COPHUB username", required=True)
    parser.add_argument("--cphubpw",help="COPHUB password", required=True)
    parser.add_argument("-email", type=str, help="Email to send the results", action="append")
    parser.add_argument('-t', action='store_true', help="Today as enddate. Otherwise the last day of the previous month is considered.")
    parser.add_argument('-n', action='store_true', help="Normal numeric check")
    parser.add_argument('-m', action='store_true', help="Monthly check with product listing.")
    return parser.parse_args()


def get_total_results(url, regex, max_retries=3, auth=('', '')):
    """Obtain the number of results found in the response message of
    a catalog query.

    :param url: catalog query url
    :param regex: regex to obtain the totalResults from the response message
    :param max_retries: repeat X times if query fails
    :param auth: tuple with (user,password) if url needs authentication
    :return: number of total results obtained by the query
    """
    for _ in range(max_retries):
        try:
            page = requests.get(url, auth=auth)
        except Exception:
            logging.exception("Error getting to URL. Retrying soon.")
            time.sleep(5)
            continue
        match = re.search(regex, str(page.content))
        if match:
            return int(match.group(1))
        else:
            logging.error("Could not obtain totalResults. Retrying soon.")
            time.sleep(5)


def diff_month(d1, d2):
    from calendar import monthrange
    from datetime import datetime, timedelta
    d1 = datetime.strptime(d1, '%Y-%m-%d')
    d2 = datetime.strptime(d2, '%Y-%m-%d')
    delta = 0
    while True:
        mdays = monthrange(d1.year, d1.month)[1]
        d1 += timedelta(days=mdays)
        if d1 <= d2:
            delta += 1
        else:
            break
    return delta


def get_list_of_results(url, regex, max_retries=10, auth=('', '')):
    """Obtain the number of results found in the response message of
    a catalog query.

    :param url: catalog query url
    :param regex: regex to obtain the totalResults from the response message
    :param max_retries: repeat X times if query fails
    :param auth: tuple with (user,password) if url needs authentication
    :return: number of total results obtained by the query
    """
    resultslist = []
    for _ in range(max_retries):
        page = requests.get(url, auth=auth)
        if not page.status_code==200:
            time.sleep(5)
            continue
        else:
            break
    if not regex is None:
        for m in re.finditer(regex, str(page.content)):
            resultslist.append(m.group(1))
    else:
        resultslist = page.text.split("\n")
        resultslist = list(filter(None, resultslist))
    return resultslist, len(resultslist)

# $("a[id^=show_]").click(function(event) {$("#extra_" + $(this).attr('id').substr(5)).slideToggle("slow"); event.preventDefault();});
def email_report(startdate,enddate,datasets):
    report_text = [
        '\nCatalog sync results (GPOD vs COPHUB)\n',
        f'{"DAY":^12}|{"G-POD Catalogue":^17}|{"COPHUB":^8}',
        '---------------------------------------'
    ]
    report_html = f"""\
        <html>
            <head><title></title></head>
            <body>
                <p><h2>Catalog sync results (GPOD vs COPHUB)</h2></p>
                <p>Run executed {str(datetime.now().strftime('%Y-%m-%d %H:%M'))}, with the following parameters:</p>
                <p>Temporal interval: {str(startdate)} to {str(enddate)}</p>
                <p>Dataset(s): {datasets}</p>
                <table border="1">
                <tr>
                    <th>DATES</th>
                    <th>G-POD Catalogue</th>
                    <th>COPHUB</th>
                </tr>
        """

    return report_text, report_html


def send_email(TO_EMAIL_LIST, report_text, report_html, attachfiles=[]):
    if TO_EMAIL_LIST:
        send_from_gmail(TO_EMAIL_LIST, 'Catalog sync results (GPOD vs COPHUB)', '\n'.join(report_text), report_html, attachfiles)


def main():
    args = setup_cmd_args()
    DAYS_BACK = 6
    username = args.cphubuser
    passw = args.cphubpw
    lstFileNames = []
    logging.basicConfig(filename=os.path.join(args.outputlist, 'gpod_cophub_sync_check.log'), level=logging.INFO,
                        format='INFO: %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info("------STARTED RUN------")

    if args.n:

        report_text = [
            '\nCatalog results for S2A_PRD_MSIL1C\n',
            f'{"DAY":^12}|{"G-POD Catalogue":^17}|{"COPHUB":^8}',
            '---------------------------------------'
        ]
        report_html = """\
        <html>
            <head><title></title></head>
            <body>
                <p>Catalog results for S2A_PRD_MSIL1C</p>
                <table border="1">
                <tr>
                    <th>DAY</th>
                    <th>G-POD Catalogue</th>
                    <th>COPHUB</th>
                </tr>
        """

        for num_day in reversed(range(DAYS_BACK)):
            cur_day_str = (datetime.today() - timedelta(days=num_day)).strftime('%Y-%m-%d')
            logging.info(f'Querying G-POD {cur_day_str}')
            results_gpod = get_total_results(
                f'http://grid-eo-catalog.esrin.esa.int/catalogue/gpod/S2A_PRD_MSIL1C/rdf/?count=1&start={cur_day_str}&stop={cur_day_str}',
                r'<os:totalResults>(\d+)</os:totalResults>')
            logging.info(f'Querying COPHUB {cur_day_str}')
            results_cophub = get_total_results(
                f'https://cophub.copernicus.eu/dhus/search?start=0&rows=1&q=(%20beginposition:[{cur_day_str}T00:00:00.000Z%20TO%20{cur_day_str}T23:59:59.999Z]%20AND%20endposition:[{cur_day_str}T00:00:00.000Z%20TO%20{cur_day_str}T23:59:59.999Z]%20)%20AND%20(platformname:Sentinel-2%20AND%20producttype:S2MSI1C)',
                r'<opensearch:totalResults>(\d+)</opensearch:totalResults>', auth=(username, passw))

            if results_gpod == results_cophub:
                report_text.append(f'{cur_day_str:^12}|{results_gpod:^17}|{results_cophub:^8}')
                report_html += f'<tr><td>{cur_day_str}</td><td align="center">{results_gpod}</td><td align="center">{results_cophub}</td></tr>\n'
            else:
                report_text.append(f'{cur_day_str:^12}|{results_gpod:^17}|{results_cophub:^8} !')
                report_html += f'<tr bgcolor="#FF3333"><td>{cur_day_str}</td><td align="center">{results_gpod}</td><td align="center">{results_cophub}</td></tr>\n'

        # Finishing up the report text/formatting
        report_text.append('\n')
        report_html += """\
                </table>
            </body>
        </html>
        """

        send_email(list(args.email), report_text, report_html)

    if args.m:
        output_dir_name = os.path.join(args.outputlist, 'reportDir' + str(uuid.uuid4()))
        os.mkdir(output_dir_name)
        if args.dataset==None:
            datasets=["S3A_SR_1_SRA_A_PREOPS","S3B_SR_1_SRA_A_NTC"]
        else:
            datasets=[args.dataset]
        months = 1
        if not args.enddate==None:
            startdate = datetime.strptime(args.startdate,'%Y-%m-%d')
            enddate = datetime.strptime(args.enddate,'%Y-%m-%d').strftime('%Y-%m-%d')
            months = diff_month(args.startdate, args.enddate) + 1
            startdate_dt = datetime(startdate.year, startdate.month, 1)
            report_text, report_html = email_report(startdate.strftime('%Y-%m-%d'), enddate, datasets)
            logging.info("Checking dataset(s) {} from {} to {}...".format(datasets, startdate.strftime('%Y-%m-%d'), enddate))
        else:
            today = datetime.today() - timedelta(days=int(args.daysback))
            # startdate_dt = datetime(today.year, today.month, 1)
            startdate_dt = datetime.strptime(args.startdate,'%Y-%m-%d')
            months = diff_month(startdate_dt.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d'))
            # d, finalday = calendar.monthrange(startdate_dt.year, startdate_dt.month)
            # enddate_dt = datetime(today.year, today.month, 1) + timedelta(days=finalday)
            if args.t:
                enddate_dt = today
            else:
                enddate_dt = datetime(today.year, today.month, 1) - timedelta(days=1)
            report_text, report_html = email_report(startdate_dt.strftime('%Y-%m-%d'), enddate_dt.strftime('%Y-%m-%d'), datasets)
            logging.info("Checking dataset(s) {} from {} to {}...".format(datasets, startdate_dt.strftime('%Y-%m-%d'), enddate_dt.strftime('%Y-%m-%d')))
            print(startdate_dt.strftime('%Y-%m-%d'), enddate_dt.strftime('%Y-%m-%d'))
        month = 1
        while months>0:
            # if not args.enddate == None:
                # enddate_dt = datetime(startdate_dt.year, startdate_dt.month, startdate_dt.day) + timedelta(days=1*365/12)
            d, finalday = calendar.monthrange(startdate_dt.year, startdate_dt.month)
            enddate_dt = datetime(startdate_dt.year, startdate_dt.month, 1) + timedelta(days=finalday)
            startdate = startdate_dt.strftime('%Y-%m-%d')
            enddate = enddate_dt.strftime('%Y-%m-%d')
            print(startdate,enddate)
            cg_txtfile = os.path.join(output_dir_name,
                                      f"unsynced_cg_files{str(startdate_dt.year)+str(startdate_dt.month)}.txt")
            gc_txtfile = os.path.join(output_dir_name,
                                      f"unsynced_gc_files{str(startdate_dt.year)+str(startdate_dt.month)}.txt")
            results_list_gpod=[]
            for ds in datasets:
                url = "http://grid-eo-catalog.esrin.esa.int/catalogue/gpod/{}/files?start={}&stop={}&count=*".format(ds,startdate,enddate)
                try:
                    results_list_gpod_i, results_list_gpod_count = get_list_of_results(url,None)
                except:
                    logging.info(f"Some problem occurred when connecting to GPOD!")
                results_list_gpod = results_list_gpod + results_list_gpod_i

            pattern_list = r'<str name="identifier">(.*?)</str>'
            pattern_total = r'<opensearch:totalResults>(\d+)</opensearch:totalResults>'
            results_list_cophub_final = []
            for ds in datasets:
                dataset = ds.replace("S3B_SR_1_SRA_A_NTC", "S3B_*").replace("S3A_SR_1_SRA_A_PREOPS", "S3A_*")
                cophubquery = f'https://cophub.copernicus.eu/dhus/search?start=0&rows=99&q=(%20beginposition:[{startdate}T00:00:00.000Z%20TO%20{enddate}T23:59:59.999Z]%20AND%20endposition:[{startdate}T00:00:00.000Z%20TO%20{enddate}T23:59:59.999Z]%20)%20AND%20(platformname:Sentinel-3 AND producttype:SR_1_SRA_A_ AND filename:{dataset} timeliness:\"Non Time Critical\")'
                try:
                    results_cophub = get_total_results(cophubquery, pattern_total, auth=(username, passw))
                except:
                    logging.info(f"Some problem occurred when connecting to COPHUB!")
                results_list_cophub_per_ds = []
                while results_cophub>=0:
                    tlimit = results_cophub
                    blimit = results_cophub - 99
                    results_cophub = blimit
                    if blimit<0: blimit = 0
                    rows = tlimit - blimit
                    cophubquery = f'https://cophub.copernicus.eu/dhus/search?start={str(blimit)}&rows={str(rows)}&q=(%20beginposition:[{startdate}T00:00:00.000Z%20TO%20{enddate}T23:59:59.999Z]%20AND%20endposition:[{startdate}T00:00:00.000Z%20TO%20{enddate}T23:59:59.999Z]%20)%20AND%20(platformname:Sentinel-3 AND producttype:SR_1_SRA_A_ AND filename:{dataset} AND timeliness:\"Non Time Critical\")'
                    try:
                        results_list_cophub, results_list_cophub_count = get_list_of_results(cophubquery, pattern_list, auth=(username, passw))
                    except:
                        logging.info(f"Some problem occurred when connecting to COPHUB!")
                    results_list_cophub_per_ds = results_list_cophub_per_ds + results_list_cophub
                results_list_cophub_final = results_list_cophub_final + results_list_cophub_per_ds
            with open(gc_txtfile, 'w+') as f:
                #find products that are in gpod catalog but not in cophub
                otext_gpod_not_in_cophub = "---Products that are in gpod catalog but not in cophub---\n"
                gpod_not_in_cophub = []
                for n in results_list_gpod:
                    if not n[:-4] in results_list_cophub_final:
                        f.write(n[:-4] + "\n")
                        otext_gpod_not_in_cophub = otext_gpod_not_in_cophub + n[:-4]+"\n"
                        gpod_not_in_cophub.append(n[:-4])

            with open(cg_txtfile, 'w+') as f:
                # find products that are in cophub but not in gpod catalogue
                otext_cophub_not_in_gpod = "---Products that are in cophub but not in gpod catalogue---\n"
                cophub_not_in_gpod = []
                for n in results_list_cophub_final:
                    if not n+".zip" in results_list_gpod:
                        f.write(n + "\n")
                        otext_cophub_not_in_gpod = otext_cophub_not_in_gpod + n+"\n"
                        cophub_not_in_gpod.append(n)

            results_cophub = len(results_list_cophub_final)
            print(len(results_list_gpod),results_cophub)
            if not len(results_list_gpod) == results_cophub:
                bgcolor = "#FF3333"
            else:
                bgcolor = "#FFFFFF"
            lstFileNames.append(gc_txtfile)
            lstFileNames.append(cg_txtfile)
            report_text.append(f'{enddate:^12}|{results_list_gpod_count:^17}|{results_cophub:^8} !')
            if not os.stat(gc_txtfile).st_size == 0:
                gc_link =  f"<a href=\"{os.path.basename(gc_txtfile)}\">{len(results_list_gpod)}</a>"
            else:
                gc_link = f"{len(results_list_gpod)}"
            if not os.stat(cg_txtfile).st_size == 0:
                cg_link = f"<a href=\"{os.path.basename(cg_txtfile)}\">{results_cophub}</a>"
            else:
                cg_link = f"{results_cophub}"
            report_html += f'<tr bgcolor="{bgcolor}"><td>{startdate} to {enddate}</td><td align="center">{gc_link}</td><td align="center">{cg_link}</td></tr>\n'
            startdate_dt = enddate_dt
            months = months - 1
            month = month + 1

        # Finishing up the report text/formatting
        report_text.append('\n')
        report_html += """\
                </table>
                <p>Please unzip the attached file and open the html contained. The opened html should have links to montlhy txt files for the following lists:</p>
                <p>unsynced_cg_files - are the files that were found in cophub but not in gpod</p>
                <p>unsynced_gc_files - are the files that were found in gpod but not in cophub</p>
            </body>
        </html>
        """
        with open(os.path.join(output_dir_name,"sync_report.html"), 'w') as f:
            f.write(report_html)
        lstFileNames.append(os.path.join(output_dir_name,"sync_report.html"))
        myzip_name = output_dir_name + '.zip'
        with zipfile.ZipFile(myzip_name, 'w') as myzip:
            for f in lstFileNames:
                os.chdir(os.path.dirname(f))
                if not os.stat(f).st_size == 0: myzip.write(os.path.basename(f))
        logging.info(f"Sending email to {', '.join(list(args.email))}")
        send_email(list(args.email), report_text, report_html, [myzip_name])
        os.chdir(args.outputlist)
        shutil.rmtree(output_dir_name)
    logging.info("------ENDED RUN------")

if __name__ == '__main__':
    main()

