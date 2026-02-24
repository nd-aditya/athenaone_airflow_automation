GENERIC_REGEX_DICT = {
    "ip": {
        "masking_value": "((IPADDRESS))",
        "regex": [
            r"\b((25[0-5]|2[0-4][0-9]|[0-1]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[0-1]?[0-9][0-9]?)\b"  # IPv4 pattern
        ],
        "processing_func": None,
    },
    "url": {
        "masking_value": "((URL))",
        "regex": [r'\bhttps?://[^\s<>"\']+', r"""(https?:\/\/[^\s"']+)"""],  # URL pattern
        "processing_func": None,
    },
    "phone_number": {
        "masking_value": "((PHONE_NUMBER))",
        "regex": [r"""(?x)                               # Enable verbose mode
(?<!\d)                            # No digit before
(?:\+1[\s.-]?)?                    # Optional +1 country code
(?:\(?\d{3}\)?[\s.-]?)              # Area code with optional parentheses
\d{3}[\s.-]?\d{4}                  # 3 digits + optional separator + 4 digits
(?!\d)                             # No digit after
"""],  # URL pattern
        "processing_func": None,
    },
    "date": {
        "masking_value": None,
        "regex": r"""
    (?:
        # ISO Format (YYYY-MM-DD)
        \b\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])(?!\w)|

        # ISO-like Format (YYYY/MM/DD)
        \b\d{4}/(?:0[1-9]|1[0-2])/(?:0[1-9]|[12]\d|3[01])(?!\w)|

        # Common American Format (MM/DD/YYYY or MM-DD-YYYY)
        \b(?:0[1-9]|1[0-2])[/-](?:0[1-9]|[12]\d|3[01])[/-]\d{4}(?!\w)|

        # European Format (DD/MM/YYYY or DD-MM-YYYY)
        \b(?:0[1-9]|[12]\d|3[01])[/-](?:0[1-9]|1[0-2])[/-]\d{4}(?!\w)|

        # Short Year Formats (MM/DD/YY or DD/MM/YY)
        \b(?:0[1-9]|1[0-2])[/-](?:0[1-9]|[12]\d|3[01])[/-]\d{2}(?!\w)|
        \b(?:0[1-9]|[12]\d|3[01])[/-](?:0[1-9]|1[0-2])[/-]\d{2}(?!\w)|

        # Textual Formats with Full Year
        # DD Month YYYY
        \b(?:0[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?\s+
        (?:January|February|March|April|May|June|July|August|September|October|November|December|
        Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)
        (?:,?\s+)\d{4}(?!\w)|

        # Month DD, YYYY
        \b(?:January|February|March|April|May|June|July|August|September|October|November|December|
        Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)
        \s+(?:0[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?(?:,?\s+)\d{4}(?!\w)|

        # Month YYYY
        \b(?:January|February|March|April|May|June|July|August|September|October|November|December|
        Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)
        \s+\d{4}(?!\w)|

        # YYYY Month
        \b\d{4}\s+
        (?:January|February|March|April|May|June|July|August|September|October|November|December|
        Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)(?!\w)|

        # Abbreviated Formats (without year)
        # Month DD
        \b(?:January|February|March|April|May|June|July|August|September|October|November|December|
        Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)
        \s+(?:0[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?(?!\w)|

        # MM DD YYYY
        \b(?:0?[1-9]|1[0-2])/(?:0?[1-9]|[12]\d|3[01])/\d{4}(?!\w)|
        
        # MM-DD-YYYY
        \b(?:0?[1-9]|1[0-2])-(?:0?[1-9]|[12]\d|3[01])-\d{4}(?!\w)|

        # MM.DD.YYYY
        \b(?:0?[1-9]|1[0-2])\.(?:0?[1-9]|[12]\d|3[01])\.\d{4}(?!\w)|

        # DD.MM.YYYY
        \b(?:0?[1-9]|[12]\d|3[01])\.(?:0?[1-9]|1[0-2])\.\d{4}(?!\w) |
        
        # YYYY.MM.DD
        \b\d{4}\.(?:0?[1-9]|1[0-2])\.(?:0?[1-9]|[12]\d|3[01])(?!\w) |
        

        # MM-DD-YYYY
        \b(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])-\d{4}(?!\w) |

        # Short Format (M/D/YY or MM/DD/YY)
        \b(?:0?[1-9]|1[0-2])[/-](?:0?[1-9]|[12]\d|3[01])[/-]\d{2}(?!\w)|

        # "%m.%d.%y"
        \b(?:0?[1-9]|1[0-2])\.(?:0?[1-9]|[12]\d|3[01])\.\d{2}(?!\w) |

        \d{1,2}/\d{1,2}/\d{4} |

        # DD Month
        \b(?:0[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?\s+
        (?:January|February|March|April|May|June|July|August|September|October|November|December|
        Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)(?!\w)

    )
""",
        "processing_func": None
    },
    "driver_license": {
        "masking_value": "((DRIVERSLICENSE))",
        "regex": [
            r"\b[A-Z]{1,3}?\d{6,12}[A-Z]?\b|\b\d{7,9}\b|\b[A-Z]{1,2}\d{7,9}\b|\b\d{3}[- ]?\d{2}[- ]?\d{4}\b"
        ],
        "processing_func": None,
    },
    "address": {
        "masking_value": "((Address))",
        "regex": [
            r"""
(?x)  # Verbose mode

(?P<street_or_pobox>
    (?:\d{1,5}[A-Za-z]{0,3}\s+[A-Za-z0-9\s.,'-]{2,40})  # Typical address
    |
    (?:P\.?\s*O\.?\s*Box\s+\d{1,6})                     # PO Box
)
[,\s]+
(?P<city>[A-Za-z\s]{2,40})  # City
[,\s]+
(?P<state>
    AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|
    MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|
    SD|TN|TX|UT|VT|VA|WA|WV|WI|WY
)
[\s,]*
(?P<zip>\d{5}(?:-\d{4})?)
"""
        ],
        "processing_func": None,
    },
    "facility_location": {
        "masking_value": "((FacilityLocation))",
        "regex": [
            r"\b(?:Amherst|Buffalo|gatescircle|gates circle|OrchardPark|Orchard Park|Batavia)\b"
        ],
        "processing_func": None,
    }
}


GENERIC_DATE_REGEX = {
    "masking_value": None,
    "regex": r"""
    (?:
        # Textual Formats (Require a Year) - Put these first 

        # 1. DD Month YYYY with optional space (e.g., "17 July 2023" or "17July2023")
        \b(?P<day1>\d{1,2})(?:st|nd|rd|th)?\s*
        (?P<month1>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        (?:,\s*|\s*)(?P<year1>\d{4})(?!\w)|

        # 2. Month DD, YYYY or Month DD YYYY with optional space (e.g., "July17, 2023" or "July17 2023")
        \b(?P<month2>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        \s*(?P<day2>\d{1,2})(?:st|nd|rd|th)?
        (?:,\s*|\s*)(?P<year2>\d{4})(?!\w)|

        # 3. Month DD YYYY with optional space (e.g., "Mar15 2023" or "Mar 15 2023")
        \b(?P<month3>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        \s*(?P<day3>\d{1,2})(?:st|nd|rd|th)?
        \s*(?P<year3>\d{4})(?!\w)|

        # 4. Month YYYY with optional space (e.g., "March2024", "Jan 2015")
        \b(?P<month4>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        \s*(?P<year4>\d{4})(?!\w)|

        # 5. Year Month with optional space (e.g., "2024June" or "2024, June")
        \b(?P<year5>\d{4})\s*(?:,\s*)?
        (?P<month5>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        (?!\w)|

        # 6. Month@Year (e.g., "June@2013")
        \b(?P<month6>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        @(?P<year6>\d{4})(?!\w)|

        # m.d.y
        \b(?:0?[1-9]|1[0-2])\.(?:0?[1-9]|[12]\d|3[01])\.\d{2}(?!\w) |

        # Numeric Formats (Require Year - 2 or 4 digits)
        \b(?P<day4>(?:0?[1-9]|1[0-2]))/(?P<month7>(?:0?[1-9]|[12]\d|3[0-1]))/(?P<year7>(?:\d{2}|\d{4}))(?!\w)|
        \b(?P<day5>(?:0?[1-9]|[12]\d|3[0-1]))/(?P<month8>(?:0?[1-9]|1[0-2]))/(?P<year8>(?:\d{2}|\d{4}))(?!\w)|
        \b(?P<day6>(?:0?[1-9]|[12]\d|3[0-1]))-(?P<month9>(?:0?[1-9]|1[0-2]))-(?P<year9>(?:\d{2}|\d{4}))(?!\w)|
        \b(?P<day7>(?:0?[1-9]|[12]\d|3[0-1]))-(?P<month10>(?:0?[1-9]|1[0-2]))-(?P<year10>(?:\d{2}|\d{4}))(?!\w)|

        # Textual Formats (No Year) - Put these last 

        # Month DD with optional space (e.g., "Sept23" or "Sept 23")
        \b(?P<month11>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        \s*(?P<day8>\d{1,2})(?:st|nd|rd|th)?\b|

        # DD Month with optional space (e.g., "23Sept" or "23 Sept")
        \b(?P<day9>\d{1,2})(?:st|nd|rd|th)?\s*
        (?P<month12>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))\b
    )
    """,
    "processing_func": None,
}