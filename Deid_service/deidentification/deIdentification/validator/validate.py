# validators = [
#     PatientCompleteness('ecw', patients_df, encounters_df),
#     LabResultCompleteness('ecw', lab_orders_df, lab_results_df),
#     InsuranceInformationCompleteness('ecw', patients_df),
# ]

# for v in validators:
#     issues = v.validate()
#     if not issues.empty:
#         print(f"Incomplete: {v.__class__.__name__}")
#         print(issues.head())
