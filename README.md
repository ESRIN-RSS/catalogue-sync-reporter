# catalogue-sync-reporter
Check status of GPOD catalog sync with COPHUB

This program will query G-POD and COPHUB on the same datasets, in order
to obtain the number of data results, compare them compile a report with the differences. This report can be emailed.

Options:

`  -h, --help            show this help message and exit`

`  --outputlist OUTPUTLIST
                        Folder to write the output lists with the un-synced
                        products. (default: c:\temp\)`
                        
`  --daysback DAYSBACK   Report with a given number of days back from today
                        (default: 0)`
                        
`  --dataset DATASET     Set which dataset to query (chose S3A_SR_1_SRA_A_PREOPS or S3B_SR_1_SRA_A_NTC) (default: None)`

`  --startdate STARTDATE
                        The Start Date (format: YYYY-MM-DD) (default:
                        2016-06-01)`
                        
`  --enddate ENDDATE     The End Date (format: YYYY-MM-DD) (default: None)`

`  --cphubuser CPHUBUSER
                        COPHUB username (default: None)`
                        
`  --cphubpw CPHUBPW     COPHUB password (default: None)`

`  -email EMAIL          Email to send the results (default: None)`

`  -t                    Today as enddate. Otherwise the last day of the
                        previous month is considered. (default: False)`
                        
`  -n                    Normal numeric check (default: False)`

`  -m                    Monthly check with product listing. (default: False)`
