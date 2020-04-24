# Calibration factor dictionary for LLC water temps
#
# Developed by:  Joseph Brewer
# for CIWS research
#
# Description:  This script is used to store the collection of calibration factors used
#               to modify temperature data collected from the LLC in conjunction with
#               flow data.  Data was collected with digital temperature sensors placed
#               directly on the pipe.
# Methods:      B:  Calculated by comparing directly with USU Building Automation System temp measurements
#               C:  Calculated by comparing directly with USU Building Automation System temp measurements
#               D:  Calculated by comparing directly with USU Building Automation System temp measurementsx
#               E:  Calculated by averaging B/C/D (BSA does not measure Bldg E water temp)
#               F:  Calculated by averaging B/C/D (BSA does not measure Bldg F water temp)
#
# Last modified: 9/17/19


def calibration_temp(BldgID):

    if "B" in BldgID:
        hotInCal = 4.47
        hotOutCal = 4.26
        coldInCal = 0
    elif "C" in BldgID:
        hotInCal = 6.5
        hotOutCal = 4.06
        coldInCal = 0
    elif "D" in BldgID:
        hotInCal = 3.5
        hotOutCal = 1.99
        coldInCal = 0
    elif "E" in BldgID:
        hotInCal = 5.79
        hotOutCal = 10.05
        coldInCal = 0
    elif "F" in BldgID:
        hotInCal = 5.79
        hotOutCal = 3.94
        coldInCal = 0

    hotInCal = float(hotInCal)
    hotOutCal = float(hotOutCal)
    coldInCal = float(coldInCal)

    calibration = [hotInCal, hotOutCal, coldInCal]


    return calibration


def calibration_flow(BldgID):
    if "B" in BldgID:
        hotInCal = 4.47
        hotInZero = 4.40
    elif "C" in BldgID:
        hotInCal = 3.38
        hotInZero = 3.31
    elif "D" in BldgID:
        hotInCal = 3.782
        hotInZero = 3.717
    elif "E" in BldgID:
        hotInCal = 3.505
        hotInZero = 3.435
    elif "F" in BldgID:
        hotInCal = 4.0
        hotInZero = 3.967

    hotInCal = float(hotInCal)
    hotInZero = float(hotInZero)


    calibration = [hotInCal, hotInZero]


    return calibration

def data_chunk(week):
    if week == 1:
        beginDate = "'2019-03-22T12:00:00Z'"
        endDate = "'2019-03-28T23:59:59Z'"
    elif week == 2:
        beginDate = "'2019-03-29T00:00:00Z'"
        endDate = "'2019-04-04T23:59:59Z'"
    elif week == 3:
        beginDate = "'2019-04-05T00:00:00Z'"
        endDate = "'2019-04-11T23:59:59Z'"
    elif week == 4:
        beginDate = "'2019-04-12T00:00:00Z'"
        endDate = "'2019-04-19T11:59:59Z'"

    dates = [beginDate, endDate]

    return dates
