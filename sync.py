import argparse
import difflib
import fnmatch
import json
import os
import re
import sys
from datetime import datetime, timezone

from colored import fg, bg, attr
import pyodbc


def ReadProcedure(cursor, objectname):
    try:
        cursor.execute(f"EXEC sp_helptext '{objectname}'")
        rows = cursor.fetchall()

        sp = []
        for row in rows:
            sp.append(row[0].rstrip("\r\n"))
        return sp
    except pyodbc.ProgrammingError:
        return None


def ReadFile(filename):
    with open(filename, encoding="utf-8") as f:
        return f.read().splitlines()


def QueryModifyDate(cursor, name):
    basename = name.split(".")[-1]
    print("QueryModifyDate", basename)
    result = cursor.execute(
        "SELECT modify_date FROM sys.objects WHERE name=?", basename)
    return result.fetchone()[0].astimezone().isoformat()


def CompareFileToDatabase(left, right):
    line = 0
    errors = []

    while line < len(left) and line < len(right) and len(errors) < config["MaxErrorsInFile"]:
        l = left[line].casefold()
        r = right[line].casefold()

        if l != r:
            errors.append({"line": line, "left": l, "right": r})

        line += 1

    if len(left) != len(right):
        errors.append({"line": 0,
                       "left": str(len(left)),
                       "right": str(len(right))})

    return errors


def FileModificationTime(path):
    t = datetime.fromtimestamp(os.stat(path).st_mtime,
                               timezone.utc)
    return t.astimezone().isoformat()


def GenerateUnifiedDiff(left, left_filename, left_date, right, right_filename, right_date):
    print(left, right, left_filename, right_filename, left_date, right_date)
    diff = difflib.unified_diff(
        left, right, left_filename, right_filename, left_date, right_date, n=3)
    sys.stdout.writelines(diff)


def DumpAsAscii(string):
    for c in string:
        print(f"{ord(c)}, ", end="")
    print()


def ReplaceVars(text, vars):
    for k, v in vars.items():
        pattern = re.compile("{" + k + "}")
        for i in range(0, len(text)):
            text[i] = re.sub(pattern, v, text[i])

    return text


def CheckOneFile(filename, vars, cursor):
    print(f"{filename}...", end="")

    left = ReadFile(os.path.join(config["BasePath"], filename))
    left = ReplaceVars(left, vars)

    objectname = os.path.splitext(filename)[0]
    right = ReadProcedure(cursor, objectname)
    if right == None:
        if args.create:
            return InsertDatabase(cursor, left)

        print(f"{fg('light_red')} FAILED (Missing){attr('reset')}")
        return False

    result = CompareFileToDatabase(left, right)
    if len(result) == 0:
        print(f"{fg('green')} OK{attr('reset')}")
        return True

    else:
        if args.update:
            return UpdateDatabase(cursor, left)

        print(f"{fg('light_red')} FAILED{attr('reset')}")

        for r in result:
            print(format(r['line'], "04"), ":",
                  r['left'], "<>", r['right'])
            if args.ascii:
                print(DumpAsAscii(r['left']))
                print(DumpAsAscii(r['right']))
        return False


def UpdateDatabase(cursor, sql):
    tmp = "\r\n".join(sql)
    tmp = re.sub("create", "alter", tmp, flags=re.IGNORECASE)
    cursor.execute(tmp)
    cursor.commit()
    print(f"{fg('yellow')} UPDATED{attr('reset')}")
    return True


def InsertDatabase(cursor, sql):
    tmp = "\r\n".join(sql)
    tmp = re.sub("alter", "create", tmp, flags=re.IGNORECASE)
    cursor.execute(tmp)
    cursor.commit()
    print(f"{fg('yellow')} CREATED{attr('reset')}")
    return True


# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--ascii", action="store_true",
                    default=False, help="Show ASCII codes of differences")
# parser.add_argument("--unified", action="store_true",
#                     default=False, help="Show differences as unified diff")
parser.add_argument("--create", action="store_true",
                    default=False, help="Create missing elements")
parser.add_argument("--update", action="store_true",
                    default=False, help="Update existing elements")
parser.add_argument("--context",
                    default=None, help="Restrict execution to one or more contexts (comma seperated)")
args = parser.parse_args()

# Read config
with open("config.json", encoding="utf-8") as cfg:
    config = json.load(cfg)

# Loop over servers
if args.context != None:
    includes = args.context.split(",")
else:
    includes = None

for server in config["Servers"]:
    if "Disabled" in server and server["Disabled"]:
        continue

    if includes and not server["Id"] in includes:
        continue

    print(f"\n{attr('bold')}Checking {server['Description']}{attr('reset')}")

    conn = pyodbc.connect("DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={0};DATABASE={1};UID={2};PWD={3}"
                          .format(server["ServerName"], server["Database"], server["Username"], server["Password"]))
    cursor = conn.cursor()

    failures = 0
    for filename in os.listdir(config["BasePath"]):
        if fnmatch.fnmatch(filename, "*.sql"):
            if not CheckOneFile(filename, server["Vars"], cursor):
                failures += 1
                # if args.unified:
                #     left_date = FileModificationTime(
                #         os.path.join(config["BasePath"], filename))
                #     left = ReadFile(os.path.join(config["BasePath"], filename))
                #     left = ReplaceVars(left, server["Vars"])

                #     objectname = os.path.splitext(filename)[0]
                #     right = ReadProcedure(cursor, objectname)
                #     right_date = QueryModifyDate(cursor, objectname)

                #     GenerateUnifiedDiff(
                #         left, "left", left_date, right, "right", right_date)

        if failures >= config["MaxFailedFiles"]:
            break
