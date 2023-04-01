import pathlib
import re
import numpy as np


_LOCAL = r'D:\KBSL\OneDrive - KBSL Information Technologies Limited\Chulax\ML\ATM withdrawal Prediction\NDB Project\data\Akkaraipatthu'

def __list_local(self, local = _LOCAL):
        directories = list()
        files = list()
        path = pathlib.Path(local)
        for content in path.iterdir():
            if content.is_dir():
                directories.append(content.name)
            else:
                files.append(content.name)
        return np.array(directories), np.array(files)

__list_local()



# files = open.
# text = "farther_mother"
# #pattern = r"-Cash Withdraw Initiated .*\n(?:.+\n)*?.*Fallback Reason  : Approved"
# pattern = r"farther"
# match = re.search(pattern, text, re.MULTILINE)
# if match:
#     print(match.group(0))
# else:
#     print("No match")

