"""This small program will query G-POD and COPHUB on the same datasets, in
order to obtain the number of data results, compile a table and email it.
"""
import logging
import re
import time
from datetime import datetime, timedelta
from typing import List

import requests

from send_email import send_from_gmail

# TO_EMAIL_LIST = ['Roberto.Cuccu@esa.int']
TO_EMAIL_LIST: List[str] = []
DAYS_BACK = 6
logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


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
    return 0


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
