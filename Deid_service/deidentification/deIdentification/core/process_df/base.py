from .rules import (
    Rules,
    PatientIDRule,
    EncounterIDRule,
    AppointmentIDRule,
    MaskRule,
    DateOffsetRule,
    RuleBase,
    StaticDateOffsetRule,
    ZIPCodeRule,
    PatientDOBRule,
)
from .unstruct.genericnotes import GenericNotesRule
import pandas as pd
from typing import Dict, Any
from nd_api_v2.models import IncrementalQueue, TableMetadata as TableModel
from deIdentification.nd_logger import nd_logger
from nd_api_v2.models.configs import get_pii_config, get_secondary_pii_configs

RULE_DISPATCHER: Dict[str, RuleBase] = {
    Rules.ENCOUNTER_ID.value: EncounterIDRule,
    Rules.APPOINTMENT_ID.value: AppointmentIDRule,
    Rules.MASK.value: MaskRule,
    Rules.DATE_OFFSET.value: DateOffsetRule,
    Rules.STATIC_OFFSET.value: StaticDateOffsetRule,
    Rules.ZIP_CODE.value: ZIPCodeRule,
    Rules.DOB.value: PatientDOBRule,
    Rules.GENERIC_NOTES.value: GenericNotesRule,
}


class DeIdentifier:
    def __init__(
        self,
        df: pd.DataFrame,
        config: list[Dict[str, Any]],
        table_obj: TableModel,
        incremental_queue_obj: IncrementalQueue,
        key_phi_columns: tuple,
        possible_patient_identifier_columns: list,
    ) -> None:
        self.df = df
        self.config = config
        self.table_obj = table_obj
        self.incremental_queue_obj = incremental_queue_obj
        self._notes_rule = None  # cache for NotesRule
        self.key_phi_columns = key_phi_columns
        self.possible_patient_identifier_columns = possible_patient_identifier_columns

    def apply_rules(self) -> pd.DataFrame:
        # Split configs into NotesRule configs and others
        notes_configs = [c for c in self.config if c.get("de_identification_rule") == Rules.NOTES.value and c["is_phi"]]
        other_configs = [c for c in self.config if not (c.get("de_identification_rule") == Rules.NOTES.value) and c["is_phi"]]

        # Process NotesRule first
        for column_config in notes_configs:
            nd_logger.info("####################################################")
            nd_logger.info(f"Applying NotesRule for column: {column_config['column_name']}, queue-id: {self.incremental_queue_obj.id}, table-id: {self.table_obj.id}")
            
            if self._notes_rule is None:
                from .unstruct.notes import NotesRule  # local import to avoid circular issues
                self._notes_rule = NotesRule(self.incremental_queue_obj, self.key_phi_columns, self.possible_patient_identifier_columns, self.table_obj)
            
            self.df = self._notes_rule.apply(self.df, column_config)

        # --- Process all other rules ---
        for column_config in other_configs:
            nd_logger.info("####################################################")
            nd_logger.info(f"Applying for column: {column_config['column_name']}, queue-id: {self.incremental_queue_obj.id}, table-id: {self.table_obj.id}")

        # Process all other rules
        pii_config = get_pii_config()
        for column_config in other_configs:
            nd_logger.info("####################################################")
            rule_type = column_config.get("de_identification_rule")

            if rule_type and rule_type.startswith("PATIENT_"):
                nd_logger.info(f"Detected dynamic rule: {rule_type}, using PatientIDRule")
                rule_impl = PatientIDRule(pii_config)
            else:
                rule_class = RULE_DISPATCHER.get(rule_type)
                rule_impl = rule_class(pii_config) if rule_class else None

            if rule_impl:
                self.df = rule_impl.apply(self.df, column_config)

        return self.df

