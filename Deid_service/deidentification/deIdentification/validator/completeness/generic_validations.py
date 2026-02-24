from .base import CompletenessValidator


class PatientCompleteness(CompletenessValidator):
    # Every patient should have at least one encounter:
    pass

class LabResultCompleteness(CompletenessValidator):
    # Every test order should have a corresponding result
    pass


class InsuranceInformationCompleteness(CompletenessValidator):
    # Patient missing payor or plan info:
    pass
