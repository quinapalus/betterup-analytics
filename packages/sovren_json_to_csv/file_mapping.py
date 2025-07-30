import fileinput

# This class extracts entries from sovren file mapping logs as records
# for a pandas DataFrame


class FileMapping:

    headers = ["resume_source", "OriginalFile", "json"]

    def __init__(self):
        self.mappings = []

    def get_mappings(self):
        return self.mappings

    def get_headers(self):
        return self.headers

    def parse(self, file, resumeSource):
        self.mappings = []
        for line in file.readlines():
            original = line[: line.find("=") - 1]
            json = line[line.find("=") + 2 : -1] + ".json"
            record = {
                "resume_source": resumeSource,
                "OriginalFile": original,
                "json": json,
            }
            self.mappings.append(record)
