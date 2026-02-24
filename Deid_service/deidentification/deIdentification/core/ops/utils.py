from nd_api.models import Table, ClientDataDump

def get_pii_config(table: Table):
    if "pii_config" in table.run_config:
        return table.run_config["pii_config"]
    elif "pii_config" in table.dump.run_config:
        return table.dump.run_config["pii_config"]
    else:
        return {}


def get_xml_config(table: Table):
    if "xml_config" in table.run_config:
        return table.run_config["xml_config"]
    elif "xml_config" in table.dump.run_config:
        return table.dump.run_config["xml_config"]
    else:
        return {}

