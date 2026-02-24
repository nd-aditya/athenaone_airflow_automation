from .base import BaseDeIdentificationRule, Rules, IgnoreRowException
from .replace import PatientIDDeIdntRule, EncounterIDDeIdntRule
from .ops import (
    DateOffsetDeIdntRule,
    ZipCodeDeIdntRule,
    DOBDeIdntRule,
    StaticDateOffsetDeIdntRule
    # TimeDeltaDeIdntRule
)
from .constants import ReusableBag
from .mask import (
    MASKDeIdntRule,
)
from .columns_type_detector import ColumnsTypeDetector

RulesMapping = {
    Rules.PATIENT_ID: PatientIDDeIdntRule,
    Rules.ENCOUNTER_ID: EncounterIDDeIdntRule,
    Rules.DATE_OFFSET: DateOffsetDeIdntRule,
    Rules.MASK: MASKDeIdntRule,
    Rules.PATIENT_DOB: DOBDeIdntRule,
    Rules.ZIP_CODE: ZipCodeDeIdntRule,
    Rules.STATIC_OFFSET: StaticDateOffsetDeIdntRule,
    Rules.OFFSET_32: StaticDateOffsetDeIdntRule,
    # Rules.REFER_PATIENT_ID: REFERPatientIDDeIdntRule,
    # Rules.REFER_PATIENT_ID_TNG: REFERPatientIDDeIdntRule,
}

# RuleToSchemaTypeMapping = {
#     "PATIENT_ID": {"type": BigInteger},
#     "ENCOUNTER_ID": {"type": String, "length": 50},
#     "PATIENT_DOB": {"type": Integer},
#     "MASK": {"type": String, "length": 200},
#     "DATE_OFFSET": {"type": String, "length": 30},
#     "ZIP_CODE": {"type": String, "length": 50},
# }
