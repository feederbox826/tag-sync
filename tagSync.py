from stashapi.stashapp import StashInterface
from stashapi.stashbox import StashBoxInterface
from tqdm import tqdm
import random
import urllib
import sqlite
from datetime import timedelta, datetime
import random
import config

EXCLUDE_PREFIX = ["r:", "c:", ".", "stashdb", "Figure", "["]

#json_input = json.loads(sys.stdin.read())
#FRAGMENT_SERVER = json_input["server_connection"]
stash = StashInterface(config.FRAGMENT_SERVER)
stashdb = StashBoxInterface(conn={ "stash": stash })

BASEURL = f"{config.FRAGMENT_SERVER['Scheme']}://{config.FRAGMENT_SERVER['Host']}:{config.FRAGMENT_SERVER['Port']}/tags"
dateThreshold = datetime.now() - timedelta(days=7)

sqlite.migrate()

print("starting")
local_only = []
name_errs = []
rename_errs = []
stashdb_alias_errs = []
local_alias_errs = []
desc_errs = []
deleted = []

# mapping functions
def map_local(localtag):
  return f"${BASEURL}/{localtag.get('id')}"

def map_remote(remotetag):
  return f"https://stashdb.org/tags/{remotetag.get('id')}"

def map_remote_local(input):
  localtag = input[0]
  remotetag = input[1]
  err = input[2]
  errmsg =  f"https://stashdb.org/tags/{remotetag.get('id')} -> {BASEURL}/{localtag.get('id')} - {err}"
  if len(input) > 3:
    errmsg += f" ({input[3]})"
  return errmsg

# syncing functions
def sync_tag(localid, field):
  localtag = stash.find_tag(localid)
  print(f"syncing {localid} {field} ({localtag.get('name')})")
  stashid = sqlite.lookup_localid(localid)[1]
  stashtag = stashdb.find_tag(stashid)
  if (field == "description"):
    stashtag_desc = stashtag.get("description")
    if (localtag.get("description") != stashtag_desc):
      stash.update_tag({ "id": localid, "description": stashtag_desc })
      tqdm.write("updated description")
  elif (field == "aliases"):
    stashtag_aliases = set(stashtag.get("aliases"))
    localtag_aliases = set(localtag.get("aliases"))
    if (stashtag_aliases != localtag_aliases):
      newset = stashtag_aliases.union(localtag_aliases)
      stash.update_tag({ "id": localid, "aliases": list(newset) })
      tqdm.write("updated aliases")
  elif (field == "name"):
    stashtag_name = stashtag.get("name")
    if (localtag.get("name") != stashtag_name):
      stash.update_tag({ "id": localid, "name": stashtag_name })
      tqdm.write("updated name")
  # quickly try to revalidate
  check = validate_tag(localtag, stashtag)
  localid = int(localtag.get("id"))
  if check["any"] == False:
    sqlite.check_id(localid)
    tqdm.write(f"✅ validated {localtag.get('name')}")

# validate tag
def validate_tag(localtag, remotetag):
  nameerr = localtag.get("name") != remotetag.get("name")
  localAlias = set(localtag.get("aliases", []))
  remoteAlias = set(remotetag.get("aliases", []))
  aliaserr = localAlias != remoteAlias
  if aliaserr:
    exclusive = localAlias ^ remoteAlias
    if all(not alias.isascii() for alias in exclusive):
      aliaserr = False
  remotedesc = remotetag.get("description")
  localdesc = localtag.get("description")
  if remotedesc is None:
    descerr = False
  else:
    descerr = localdesc != remotedesc
  return {
    "name": nameerr,
    "aliases": aliaserr,
    "description": descerr,
    "any": nameerr or aliaserr or descerr
  }

# diff handlers
def get_rename_diff(localtag, remotetag):
  localname = localtag.get("name")
  remotename = remotetag.get("name")
  if localname in remotetag.get("aliases"):
    return [localname, remotename]

def get_alias_diff(localtag, remotetag):
  localAlias = set(localtag.get("aliases"))
  remoteAlias = set(remotetag.get("aliases"))
  exclusive = localAlias ^ remoteAlias
  exclusive = list(filter(lambda alias: alias.isascii(), exclusive))
  if localAlias.issubset(remoteAlias):
    return [exclusive, "stashdb"]
  elif remoteAlias.issubset(localAlias):
    return [exclusive, "local"]

def get_desc_diff(localtag, remotetag):
  localDesc = localtag.get("description")
  remoteDesc = remotetag.get("description")
  if localDesc != remoteDesc:
    if (localDesc.rstrip() == remoteDesc):
      return "whitespace"
    else:
      return "mismatch"

# find remote tag and sync if possible
def get_remote_tag(localtag):
  lookup_tag = sqlite.lookup_localid(localtag.get("id"))
  if lookup_tag:
    return stashdb.find_tag(lookup_tag[1])
  else:
    remotetag = stashdb.find_tag(localtag.get("name"))
    if remotetag is None:
      return None
    else:
      try:
        sqlite.add_ids(localtag.get("id"), remotetag.get("id"))
      except:
        print(f"failed to add {localtag.get('name')} to db with id {remotetag.get('id')}")
      return remotetag

def tag_checked(localtag):
  lookup_tag = sqlite.lookup_localid(localtag.get("id"))
  if lookup_tag:
    return lookup_tag[2] is not None
  else:
    return False

def easy_whitespace_fix(localtag, remotetag):
  localid = int(localtag.get("id"))
  localname = localtag.get("name")
  localDesc = localtag.get("description")
  remoteDesc = remotetag.get("description")
  if localDesc.rstrip() == remoteDesc:
    tqdm.write("✂️ rstrip desc " + localname)
    sync_tag(localid, "description")
  elif not localDesc and remoteDesc:
    tqdm.write("➕ add desc " + localname)
    sync_tag(localid, "description")

def easy_title_fix(localtag, remotetag):
  localname = localtag.get("name")
  remotename = remotetag.get("name")
  if localname.lower() == remotename.lower():
    print("🔠 title case " + localname)
    sync_tag(localtag.get("id"), "name")

# high level match
def match_tags():
  tags = sqlite.get_unchecked(dateThreshold)
  for tag in tqdm(tags):
    localid = int(tag[0])
    localtag = stash.find_tag(localid)
    if not localtag:
        # does not exist, delete
        sqlite.delete_id(localid)
        sqlite.remove_error(localid)
        continue
    name = localtag.get("name")
    present = tag_checked(localtag)
    iserror = sqlite.lookup_error(localid)
    if (present and iserror):
      sqlite.remove_error(localid)
      continue
    elif (present or iserror):
      continue
    remotetag = get_remote_tag(localtag)
    if remotetag is None:
      tqdm.write(f"❓ not found {name}")
      sqlite.add_error(localid, True, name)
      continue
    # check if remote tag still exists
    if remotetag.get("deleted"):
      tqdm.write(f"❌ remote tag deleted {name}")
      deleted.append(localtag)
      continue
    result = validate_tag(localtag, remotetag)
    if result["description"]:
      easy_whitespace_fix(localtag, remotetag)
    if result["name"]:
      easy_title_fix(localtag, remotetag)
    # no errors, validated
    if result["any"] == False:
      sqlite.check_id(localid)
      tqdm.write(f"✅ validated {name}")
      continue
    else:
      tqdm.write(f"❌ validation errors: {name}")

# check tag
def check_tags(localtag, remotetag):
  name = localtag.get("name")
  tagid = int(localtag.get("id"))
  # run validation
  result = validate_tag(localtag, remotetag)
  # no errors, validated
  if result["any"] == False:
    sqlite.check_id(tagid)
    return
  # check if name differs
  if result["name"] == True:
    err = get_rename_diff(localtag, remotetag)
    if err:
      print(f"rename mismatch for {name}")
      errlog = [localtag, remotetag, "rename"]
      rename_errs.append(errlog)
      print(map_remote_local(errlog))
    else:
      print(f"name mismatch for {name}")
      errlog = [localtag, remotetag, "mismatch"]
      name_errs.append(errlog)
      print(map_remote_local(errlog))
  # check if aliases match
  if result["aliases"] == True:
    alias_err = get_alias_diff(localtag, remotetag)
    if alias_err:
      print(f"alias mismatch for {name}")
      errlog = [localtag, remotetag, alias_err[0], alias_err[1]]
      if alias_err[1] == "stashdb":
        stashdb_alias_errs.append(errlog)
        print(map_remote_local(errlog))
      else:
        local_alias_errs.append(errlog)
        print(map_remote_local(errlog))
  # check if description matches
  if result["description"] == True:
    desc_err = get_desc_diff(localtag, remotetag)
    if desc_err == "whitespace" == True:
      print(f"local description has trailing whitespace for {name}, repairing automatically")
      sync_tag(tagid, "description")
    elif desc_err == "mismatch":
      print(f"description mismatch for {name}")
      errlog = [localtag, remotetag, "mismatch"]
      desc_errs.append(errlog)
      print(map_remote_local(errlog))

# scan and repair
def scan_tags():
  # pull tags locally, iterate
  localtags = stash.find_tags(fragment="id name description aliases")
  no_exist_tags = [ tag for tag in localtags if not (sqlite.lookup_error(int(tag.get("id"))) or sqlite.lookup_localid(int(tag.get("id")))) ]
  random.shuffle(no_exist_tags)
  print(f"checking {len(no_exist_tags)} tags")
  for tag in tqdm(no_exist_tags):
    # get remote tag equivalent
    remotetag = get_remote_tag(tag)
    # no remote tag, skip
    if remotetag is None:
      local_only.append(tag)
      sqlite.add_error(int(tag.get("id")), True, tag.get("name"))
      continue
    elif remotetag:
      print(f"found {tag.get('name')} {tag.get('id')} {remotetag.get('id')}")
  printerr()

def scan_unchecked_tags():
  tags = sqlite.get_unchecked(dateThreshold)
  random.shuffle(tags)
  print(f"checking {len(tags)} tags")
  for tag in tags[:]:
    localtag = stash.find_tag(int(tag[0]))
    remotetag = stashdb.find_tag(tag[1])
    check_tags(localtag, remotetag)
  printerr()
  create_run_file()

def scan_repair_local():
  localonly = sqlite.getall_errors()
  for tag in localonly:
    localtag = stash.find_tag(int(tag[0]))
    if not localtag:
      print(f"tag {tag[0]} does not exist")
      sqlite.delete_id(tag[0])
      sqlite.remove_error(tag[0])
      continue
    local_only.append(localtag)
  create_local_repair()

# print errors
def printerr():
  # print out all errors
  print("local only:")
  print(list(map(map_local, local_only)))
  print("name mismatch:")
  print(list(map(map_remote_local, name_errs)))
  print("rename mismatch:")
  print(list(map(map_remote_local, rename_errs)))
  print("stashdb alias mismatch:")
  print(stashdb_alias_errs)
  print(list(map(map_remote_local, stashdb_alias_errs)))
  # print("local alias mismatch:")
  # print(list(map(map_remote_local, local_alias_errs)))
  print("description mismatch:")
  print(list(map(map_remote_local, desc_errs)))
  print("deleted:")
  print(list(map(map_remote, deleted)))

def starts_prefix(tagname):
  for PREFIX in EXCLUDE_PREFIX:
    if tagname.startswith(PREFIX):
      return True
  return False

# create repair files
def create_local_repair():
  with open("repair-local.py", "w", encoding="utf-8") as f:
    f.write('import tagSync as sync\n')
    f.write(f"# local only:\n")
    for tag in local_only:
      if tag.get("ignore_auto_tag"):
        continue
      tagname = tag.get('name')
      if starts_prefix(tagname):
        continue
      urlencname = urllib.parse.quote(tagname)
      f.write(f"# {tagname}\n")
      f.write(f"# https://stashdb.org/tags?query={urlencname}%22\n")
      f.write(f"sync.manual_match({tag.get('id')}, \"\")\n\n")

def create_run_file():
  with open("repair.py", "w", encoding="utf-8") as f:
    f.write('import tagSync as sync\n')
    f.write("# name mismatch:\n")
    for tag in name_errs:
      f.write(f"# {map_remote_local(tag)}\n")
      f.write(f"#sync.sync_tag({tag[0].get('id')}, \"name\")\n")
    f.write("# likely renamed tags:\n")
    for tag in rename_errs:
      f.write(f"# {map_remote_local(tag)}\n")
    f.write("# stashdb extra aliases:\n")
    for tag in stashdb_alias_errs:
      f.write(f"# {tag[0].get('name')}\n")
      f.write(f"# {map_remote_local(tag)}\n")
      f.write(f"#sync.sync_tag({tag[0].get('id')}, \"aliases\")\n\n")
    f.write("# local extra aliases:\n")
    for tag in local_alias_errs:
      f.write(f"# {tag[0].get('name')}\n")
      f.write(f"# {map_remote_local(tag)}\n")
      f.write(f"#sync.sync_tag({tag[0].get('id')}, \"aliases\")\n\n")
    f.write("# description mismatch:\n")
    for tag in desc_errs:
      f.write(f"# {map_remote_local(tag)}\n")
      f.write(f"# local: {tag[0].get('description')}\n")
      f.write(f"# remote: {tag[1].get('description')}\n")
      f.write(f"#sync.sync_tag({tag[0].get('id')}, \"description\")\n\n")

# manual match function
def manual_match(localid, stashid):
  if (stashid != ""):
    print(f"manual match {localid} {stashid}")
    sqlite.add_ids(localid, stashid)

if __name__ == "__main__":
  pass
  # get new tags
  scan_tags()
  # shallow high level matching only with simple rstrip and title case fixes
  match_tags()
  # full validation of tags, with repair.py file generated
  scan_unchecked_tags()
  # only prints out tags that only exist locally
  scan_repair_local()