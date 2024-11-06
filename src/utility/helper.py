
catalog = "project"
br_schema = "bronze"

#Return the rules matching the tag as a format ready for DLT annotation.

def get_rules(tablename, quality):
  """
    loads data quality rules from csv file
    :param tablename: tablename to match
    :param quality: logical quality level
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  df = spark.table(f"{catalog}.{br_schema}.expectations").where(f"tablename = '{tablename}' and quality ='{quality}'")
  for row in df.collect():
    rules[row['name']] = row['constraint']
  return rules

