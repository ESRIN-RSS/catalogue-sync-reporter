"""This small program will query G-POD and COPHUB on the same datasets, in
order to obtain the number of data results, compile a table and email it.
"""
import logging
import re, os
import time
from datetime import datetime, timedelta
from typing import List
import argparse

import requests

from send_email import send_from_gmail


def setup_cmd_args():
    """Setup command line arguments."""
    parser = argparse.ArgumentParser(description="This small program will query G-POD and COPHUB on the same datasets, in order to obtain the number of data results, compile a table and email it.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # parser.add_argument("root_dir", help="The root directory containing data to check")
    # parser.add_argument("--workspace", help="Set Workspace manually")
    parser.add_argument("--outputlist", help="File to write the output list with the un-synced products.")
    parser.add_argument("--daysback", help="Report with a given number of days back from today")
    parser.add_argument("--dataset", help="Set Workspace manually", default="S3B_SR_1_SRA_A_NTC")
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
            logger.exception("Error getting to URL. Retrying soon.")
            time.sleep(5)
            continue
        match = re.search(regex, str(page.content))
        if match:
            return int(match.group(1))
        else:
            logger.error("Could not obtain totalResults. Retrying soon.")
            time.sleep(5)



def get_list_of_results(url, regex, max_retries=3, auth=('', '')):
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
        try:
            page = requests.get(url, auth=auth)
        except Exception:
            logger.exception("Error getting to URL. Retrying soon.")
            time.sleep(5)
            continue
    if not regex is None:
        for m in re.finditer(regex, str(page.content)):
            resultslist.append(m.group(1))
    else:
        resultslist = page.text.split("\n")
        resultslist = list(filter(None, resultslist))
    return resultslist, len(resultslist)


def main():
    args = setup_cmd_args()

    if args.n:
        # TO_EMAIL_LIST = ['Roberto.Cuccu@esa.int']
        TO_EMAIL_LIST: List[str] = []
        DAYS_BACK = 6
        logger = logging.getLogger()
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

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
            logger.info(f'Querying G-POD {cur_day_str}')
            results_gpod = get_total_results(
                f'http://grid-eo-catalog.esrin.esa.int/catalogue/gpod/S2A_PRD_MSIL1C/rdf/?count=1&start={cur_day_str}&stop={cur_day_str}',
                r'<os:totalResults>(\d+)</os:totalResults>')
            logger.info(f'Querying COPHUB {cur_day_str}')
            results_cophub = get_total_results(
                f'https://cophub.copernicus.eu/dhus/search?start=0&rows=1&q=(%20beginposition:[{cur_day_str}T00:00:00.000Z%20TO%20{cur_day_str}T23:59:59.999Z]%20AND%20endposition:[{cur_day_str}T00:00:00.000Z%20TO%20{cur_day_str}T23:59:59.999Z]%20)%20AND%20(platformname:Sentinel-2%20AND%20producttype:S2MSI1C)',
                r'<opensearch:totalResults>(\d+)</opensearch:totalResults>', auth=('ecadau', 'gj27k?Q$'))

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

        if TO_EMAIL_LIST:
            logger.info(f"Sending email to {', '.join(TO_EMAIL_LIST)}")
            send_from_gmail(TO_EMAIL_LIST, 'S2A_PRD_MSIL1C Catalogue report', '\n'.join(report_text), report_html)
        logger.info('\n'.join(report_text))
        logger.info("Done.")

    if args.m:
        today = datetime.today() - timedelta(days=int(args.daysback))
        startdate = datetime(today.year, today.month, 1).strftime('%Y-%m-%d')
        enddate = today.strftime('%Y-%m-%d')
        username = 'ecadau'
        passw = 'gj27k?Q$'
        url = "http://grid-eo-catalog.esrin.esa.int/catalogue/gpod/{}/files?start={}&stop={}&count=*".format(args.dataset,startdate,enddate)
        results_list_gpod, results_list_gpod_count = get_list_of_results(url,None)
        pattern_list = r'<str name="identifier">(.*?)</str>'
        pattern_total = r'<opensearch:totalResults>(\d+)</opensearch:totalResults>'
        cophubquery = f'https://cophub.copernicus.eu/dhus/search?start=0&rows=99&q=(%20beginposition:[{startdate}T00:00:00.000Z%20TO%20{enddate}T23:59:59.999Z]%20AND%20endposition:[{startdate}T00:00:00.000Z%20TO%20{enddate}T23:59:59.999Z]%20)%20AND%20(platformname:Sentinel-3 AND producttype:SR_1_SRA_A_ AND timeliness:\"Non Time Critical\")'
        results_cophub = get_total_results(cophubquery, pattern_total, auth=(username, passw))
        results_list_cophub_final = []
        while results_cophub>=0:
            tlimit = results_cophub
            blimit = results_cophub - 99
            results_cophub = blimit
            if blimit<0: blimit = 0
            rows = tlimit - blimit
            cophubquery = f'https://cophub.copernicus.eu/dhus/search?start={str(blimit)}&rows={str(rows)}&q=(%20beginposition:[{startdate}T00:00:00.000Z%20TO%20{enddate}T23:59:59.999Z]%20AND%20endposition:[{startdate}T00:00:00.000Z%20TO%20{enddate}T23:59:59.999Z]%20)%20AND%20(platformname:Sentinel-3 AND producttype:SR_1_SRA_A_ AND timeliness:\"Non Time Critical\")'
            results_list_cophub, results_list_cophub_count = get_list_of_results(cophubquery, pattern_list, auth=(username, passw))
            results_list_cophub_final = results_list_cophub_final + results_list_cophub
        #find products that are in gpod catalog but not in cophub
        with open(args.outputlist, 'w') as f:
            f.write(f"---Analysing period from {startdate} to {enddate}---\n")
            f.write("---Products that are in gpod catalog but not in cophub---\n")
            gpod_not_in_cophub = []
            for n in results_list_gpod:
                if not n[:-4] in results_list_cophub_final:
                    f.write(n[:-4]+"\n")
                    gpod_not_in_cophub.append(n[:-4])
            # find products that are in cophub but not in gpod catalogue
            f.write("---Products that are in cophub but not in gpod catalogue---\n")
            cophub_not_in_gpod = []
            for n in results_list_cophub_final:
                if not n+".zip" in results_list_gpod:
                    f.write(n+"\n")
                    cophub_not_in_gpod.append(n)

if __name__ == '__main__':
    main()

