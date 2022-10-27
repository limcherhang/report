from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import sys
gauth = GoogleAuth()
gauth.LocalWebserverAuth() # Creates local webserver and auto handles authentication.
drive = GoogleDrive(gauth)
try:
    name = '2022-10-01_bet_round_rtp_1_day.xlsx'  # It's the file which you'll upload
    file = drive.CreateFile()  # Create GoogleDriveFile instance
    file.SetContentFile(name)
    file.Upload()
except :
    print("Unexpected error:", sys.exc_info()[0])