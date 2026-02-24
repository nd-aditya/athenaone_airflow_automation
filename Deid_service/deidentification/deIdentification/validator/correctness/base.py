class CorrectnessValidator:
    def __init__(self, emr_name, df, config=None):
        self.emr_name = emr_name
        self.df = df
        self.config = config or {}

    def emr_type(self):
        return self.emr_name

    def validate(self):
        raise NotImplementedError("Subclasses must implement this method.")