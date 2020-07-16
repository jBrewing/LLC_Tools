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
        hotInCal = 3.081 #prev val was 5.79
        hotOutCal = 6.431 # prev val was 10.05
        coldInCal = 0
    elif "F" in BldgID:
        hotInCal = 3.622 # prev val was 5.79
        hotOutCal = 2.205 # prev val was 3.94
        coldInCal = 0

    hotInCal = float(hotInCal)
    hotOutCal = float(hotOutCal)
    coldInCal = float(coldInCal)

    calibration = [hotInCal, hotOutCal, coldInCal]


    return calibration