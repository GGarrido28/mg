import os

from mg.utils.settings import FILE_ADDRESES


class Cleaner:
    def __init__(self):
        self.download_path = FILE_ADDRESES.get("download_directory")
        self.dk_entries_file = "DKEntries"
        self.dk_salaries_file = "DKSalaries"
        self.fd_entries_file = "entries-upload-template"
        self.ob_entries_file = "entries_ownersbox"
        self.ob_swaps_file = "GlobalSwap-Lineups"
        self.lineups_file = "lineups_"
        self.projections_file = "Fantasy and Ownership Projections"
        self.fd_file = "FanDuel-"
        self.dk_contest_standings = "contest-standings-"
        
    def remove_files(self):
        files = [
            self.dk_entries_file,
            self.dk_salaries_file,
            self.fd_entries_file,
            self.ob_entries_file,
            self.lineups_file,
            self.ob_swaps_file,
            self.projections_file,
            self.fd_file,
            self.dk_contest_standings,
        ]
        for file in os.listdir(self.download_path):
            for f in files:
                if f in file:
                    file_path = os.path.join(self.download_path, file)
                    try:
                        os.remove(file_path)
                    except FileNotFoundError:
                        pass


if __name__ == "__main__":
    cleaner = Cleaner()
    cleaner.remove_files()
