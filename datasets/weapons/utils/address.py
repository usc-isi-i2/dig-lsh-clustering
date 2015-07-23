#!/usr/bin/env python
import re
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

us_states_names = [
    "ALABAMA", "ALASKA", "ARIZONA", "ARKANSAS", "CALIFORNIA", "COLORADO", "CONNECTICUT", "DELAWARE", "FLORIDA",
    "GEORGIA", "HAWAII", "IDAHO", "ILLINOIS", "INDIANA", "IOWA", "KANSAS", "KENTUCKY", "LOUISIANA", "MAINE", "MARYLAND",
    "MASSACHUSETTS", "MICHIGAN", "MINNESOTA", "MISSISSIPPI", "MISSOURI", "MONTANA", "NEBRASKA", "NEVADA",
    "NEWHAMPSHIRE", "NEWJERSEY", "NEWMEXICO", "NEWYORK", "NORTHCAROLINA", "NORTHDAKOTA", "OHIO", "OKLAHOMA", "OREGON",
    "PENNSYLVANIA", "RHODEISLAND", "SOUTHCAROLINA", "SOUTHDAKOTA", "TENNESSEE", "TEXAS", "UTAH", "VERMONT", "VIRGINIA",
    "WASHINGTON", "WESTVIRGINIA", "WISCONSIN", "WYOMING"
]

us_states_names_readable = [
     "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida",
    "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
    "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia",
    "Washington", "West Virginia", "Wisconsin", "Wyoming"
]

us_states_codes = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA",
    "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

def alphaOnly(x):
    "Remove non-alphabetic chars from the string x"
    return re.sub('[^A-Za-z]+', '', x)

def standardize_state_code(state):
    if(len(state) == 0):
        return ''

    codesArr = []
    namesArr = []


    namesArr = us_states_names
    codesArr = us_states_codes


    if (len(codesArr) > 0):
        state = state.upper()
        if (len(state) == 2):
            try:
                idx = codesArr.index(state)
                return state
            except ValueError:
                idx = -1

        try:
            state = alphaOnly(state).upper()
            idx = namesArr.index(state)
            code = codesArr[idx]
            return code
        except ValueError:
            return ''

    return state


def standardize_state_name(state):
    if(len(state) == 0):
        return ''

    codesArr = us_states_codes
    namesArr = us_states_names
    namesReadableArr = us_states_names_readable

    if (len(codesArr) > 0):
        state = state.upper()
        if (len(state) == 2):
            try:
                idx = codesArr.index(state)
                return namesReadableArr[idx]
            except ValueError:
                idx = -1

        try:
            state = alphaOnly(state).upper()
            idx = namesArr.index(state)
            return namesReadableArr[idx]
        except ValueError:
            return ''

    return state
