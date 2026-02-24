"""
Output Manager for PHI De-identification Pipeline
Handles CSV output generation and result formatting
"""

import os
import csv
import logging
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import asdict


class OutputManager:
    """Manages output generation for PHI classification results"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize output manager

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.output_file = self.config.get('pipeline', {}).get('output_file', 'phi_classification_results.csv')
        self.results = []

    def add_result(self, db_name: str, table_name: str, column_name: str, is_phi: str, phi_rule: str,
                   validation_passed: bool = None, confidence: float = None,
                   pipeline_remark: Optional[str] = None, llm_phi_type: Optional[str] = None) -> None:
        """
        Add a classification result

        Args:
            db_name: Name of the database
            table_name: Name of the table
            column_name: Name of the column
            is_phi: Whether column contains PHI ('yes' or 'no')
            phi_rule: PHI rule/type if applicable
            validation_passed: Whether validation tool confirmed the classification
            confidence: Confidence score if available
            pipeline_remark: Any pipeline remark
            llm_phi_type: LLM PHI type if available
        """
        result = {
            'db_name': db_name,
            'table_name': table_name,
            'column_name': column_name,
            'is_phi': is_phi,
            'phi_rule': phi_rule,
            'validation_passed': validation_passed,
            'confidence': confidence,
            'pipeline_remark': pipeline_remark,
            'llm_phi_type': llm_phi_type,
            'timestamp': datetime.now().isoformat()
        }
        self.results.append(result)
        self.logger.debug(f"Added result: {db_name}.{table_name}.{column_name} -> {is_phi}, {phi_rule}")

    def add_batch_results(self, results: List[Dict[str, Any]]) -> None:
        """
        Add multiple classification results

        Args:
            results: List of result dictionaries
        """
        for result in results:
            self.add_result(
                db_name=result.get('db_name', ''),
                table_name=result.get('table_name', ''),
                column_name=result.get('column_name', ''),
                is_phi=result.get('is_phi', 'no'),
                phi_rule=result.get('phi_rule', ''),
                validation_passed=result.get('validation_passed'),
                confidence=result.get('confidence'),
                pipeline_remark=result.get('pipeline_remark'),
                llm_phi_type=result.get('llm_phi_type')
            )
        self.logger.info(f"Added {len(results)} batch results")

    def generate_csv_output(self, output_path: Optional[str] = None) -> str:
        """
        Generate CSV output file

        Args:
            output_path: Custom output path (optional)

        Returns:
            Path to generated CSV file
        """
        try:
            file_path = output_path or self.output_file

            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)

            # Define CSV headers
            headers = ['DB_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'IS_PHI', 'DE_IDENTIFICATION_RULE', 'MASK_VALUE', 'PIPELINE_REMARK']

            # Pre-compute table-level presence of patient/encounter rules
            table_has_patient_or_encounter = {}
            for result in self.results:
                table = result.get('table_name')
                rule = (result.get('phi_rule') or '')
                passed = result.get('validation_passed') is True
                has_pe = False
                if rule:
                    upper_rule = str(rule).upper()
                    if (upper_rule.startswith('PATIENT_') or upper_rule == 'ENCOUNTER_ID') and passed:
                        has_pe = True
                table_has_patient_or_encounter[table] = table_has_patient_or_encounter.get(table, False) or has_pe

            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()

                for result in self.results:
                    # Derive final phi_rule with table-level adjustments
                    table = result.get('table_name')
                    base_rule = (result.get('phi_rule') or '').strip()
                    # llm_type = (result.get('llm_phi_type') or '').strip()

                    upper_base_rule = base_rule.upper()
                    # lower_llm_type = llm_type.lower()

                    has_pe = table_has_patient_or_encounter.get(table, False)

                    final_rule = ''
                    # Normalize common validator rules to lower for output
                
                     # Only set rules if IS_PHI is 'yes'
                    if result.get('is_phi') == 'yes':
                        # Normalize common validator rules to lower for output
                        if upper_base_rule == 'DATE_OFFSET':
                            final_rule = 'DATE_OFFSET' if has_pe else 'STATIC_OFFSET'
                        elif upper_base_rule == 'NOTES':
                            final_rule = 'NOTES' if has_pe else 'GENERIC_NOTES'
                        else:
                            final_rule = base_rule.upper()
                    # If IS_PHI is 'no', final_rule remains empty

                    if final_rule == 'MASK':
                        mask_value = result.get('column_name', '')
                    else:
                        mask_value = ''

                    # Write only the required columns
                    row = {
                        'DB_NAME': result.get('db_name', ''),
                        'TABLE_NAME': result.get('table_name', ''),
                        'COLUMN_NAME': result.get('column_name', ''),
                        'IS_PHI': result.get('is_phi', ''),
                        'DE_IDENTIFICATION_RULE': final_rule,
                        'MASK_VALUE': mask_value,
                        'PIPELINE_REMARK': result.get('pipeline_remark', '')
                    }
                    writer.writerow(row)

            self.logger.info(f"CSV output generated: {file_path} ({len(self.results)} records)")
            return file_path

        except Exception as e:
            self.logger.error(f"Failed to generate CSV output: {str(e)}")
            raise

    def generate_detailed_csv_output(self, output_path: Optional[str] = None) -> str:
        """
        Generate detailed CSV output with all fields

        Args:
            output_path: Custom output path (optional)

        Returns:
            Path to generated detailed CSV file
        """
        try:
            base_path = output_path or self.output_file
            detailed_path = base_path.replace('.csv', '_detailed.csv')

            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(detailed_path)), exist_ok=True)

            # Define detailed CSV headers
            headers = [
                'db_name', 'table_name', 'column_name', 'is_phi', 'phi_rule',
                'validation_passed', 'confidence', 'pipeline_remark', 'llm_phi_type', 'timestamp'
            ]

            # Write detailed CSV file
            with open(detailed_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()

                for result in self.results:
                    row = {h: result.get(h, '') for h in headers}
                    writer.writerow(row)

            self.logger.info(f"Detailed CSV output generated: {detailed_path} ({len(self.results)} records)")
            return detailed_path

        except Exception as e:
            self.logger.error(f"Failed to generate detailed CSV output: {str(e)}")
            raise

    def generate_summary_report(self, output_path: Optional[str] = None) -> str:
        """
        Generate summary report of classification results

        Args:
            output_path: Custom output path (optional)

        Returns:
            Path to generated summary report
        """
        try:
            base_path = output_path or self.output_file
            summary_path = base_path.replace('.csv', '_summary.txt')

            # Calculate summary statistics
            total_columns = len(self.results)
            phi_columns = len([r for r in self.results if r.get('is_phi') == 'yes'])
            non_phi_columns = total_columns - phi_columns

            # Count by PHI type
            phi_type_counts = {}
            validation_stats = {'passed': 0, 'failed': 0, 'not_validated': 0}

            for result in self.results:
                if result.get('is_phi') == 'yes' and result.get('phi_rule'):
                    phi_type = result.get('phi_rule')
                    phi_type_counts[phi_type] = phi_type_counts.get(phi_type, 0) + 1

                # Validation statistics
                if result.get('validation_passed') is True:
                    validation_stats['passed'] += 1
                elif result.get('validation_passed') is False:
                    validation_stats['failed'] += 1
                else:
                    validation_stats['not_validated'] += 1

            # Count by table
            table_counts = {}
            for result in self.results:
                table = result.get('table_name', '')
                if table not in table_counts:
                    table_counts[table] = {'total': 0, 'phi': 0}
                table_counts[table]['total'] += 1
                if result.get('is_phi') == 'yes':
                    table_counts[table]['phi'] += 1

            # Generate summary report
            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write("PHI DE-IDENTIFICATION PIPELINE - SUMMARY REPORT\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

                f.write("OVERALL STATISTICS\n")
                f.write("-" * 20 + "\n")
                f.write(f"Total Columns Analyzed: {total_columns}\n")
                f.write(f"PHI Columns Identified: {phi_columns}\n")
                f.write(f"Non-PHI Columns: {non_phi_columns}\n")
                f.write(f"PHI Percentage: {(phi_columns/total_columns)*100:.1f}%\n\n" if total_columns else "PHI Percentage: 0.0%\n\n")

                if phi_type_counts:
                    f.write("PHI TYPES BREAKDOWN\n")
                    f.write("-" * 20 + "\n")
                    for phi_type, count in sorted(phi_type_counts.items()):
                        f.write(f"{str(phi_type).capitalize()}: {count}\n")
                    f.write("\n")

                f.write("VALIDATION STATISTICS\n")
                f.write("-" * 20 + "\n")
                f.write(f"Validation Passed: {validation_stats['passed']}\n")
                f.write(f"Validation Failed: {validation_stats['failed']}\n")
                f.write(f"Not Validated: {validation_stats['not_validated']}\n\n")

                f.write("TABLE BREAKDOWN\n")
                f.write("-" * 20 + "\n")
                for table, counts in sorted(table_counts.items()):
                    phi_pct = (counts['phi']/counts['total'])*100 if counts['total'] > 0 else 0
                    f.write(f"{table}:\n")
                    f.write(f"  Total Columns: {counts['total']}\n")
                    f.write(f"  PHI Columns: {counts['phi']} ({phi_pct:.1f}%)\n\n")

            self.logger.info(f"Summary report generated: {summary_path}")
            return summary_path

        except Exception as e:
            self.logger.error(f"Failed to generate summary report: {str(e)}")
            raise

    def get_results_dataframe(self) -> pd.DataFrame:
        """
        Get results as pandas DataFrame

        Returns:
            DataFrame containing all results
        """
        try:
            df = pd.DataFrame(self.results)
            return df
        except Exception as e:
            self.logger.error(f"Failed to create results DataFrame: {str(e)}")
            return pd.DataFrame()

    def filter_results(self, is_phi: Optional[str] = None, phi_rule: Optional[str] = None,
                      table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Filter results based on criteria

        Args:
            is_phi: Filter by PHI status ('yes' or 'no')
            phi_rule: Filter by PHI rule/type
            table_name: Filter by table name

        Returns:
            Filtered list of results
        """
        filtered_results = self.results.copy()

        if is_phi is not None:
            filtered_results = [r for r in filtered_results if r.get('is_phi') == is_phi]

        if phi_rule is not None:
            filtered_results = [r for r in filtered_results if r.get('phi_rule') == phi_rule]

        if table_name is not None:
            filtered_results = [r for r in filtered_results if r.get('table_name') == table_name]

        return filtered_results

    def get_phi_columns(self) -> List[Dict[str, Any]]:
        """
        Get all columns identified as PHI

        Returns:
            List of PHI column results
        """
        return self.filter_results(is_phi='yes')

    def get_non_phi_columns(self) -> List[Dict[str, Any]]:
        """
        Get all columns identified as non-PHI

        Returns:
            List of non-PHI column results
        """
        return self.filter_results(is_phi='no')

    def export_results(self, format: str = 'csv', output_path: Optional[str] = None) -> str:
        """
        Export results in specified format

        Args:
            format: Export format ('csv', 'detailed_csv', 'json', 'excel')
            output_path: Custom output path (optional)

        Returns:
            Path to exported file
        """
        try:
            if format == 'csv':
                return self.generate_csv_output(output_path)
            elif format == 'detailed_csv':
                return self.generate_detailed_csv_output(output_path)
            elif format == 'json':
                return self._export_json(output_path)
            elif format == 'excel':
                return self._export_excel(output_path)
            else:
                raise ValueError(f"Unsupported export format: {format}")

        except Exception as e:
            self.logger.error(f"Failed to export results in {format} format: {str(e)}")
            raise

    def _export_json(self, output_path: Optional[str] = None) -> str:
        """Export results as JSON"""
        import json

        base_path = output_path or self.output_file
        json_path = base_path.replace('.csv', '.json')

        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, default=str)

        self.logger.info(f"JSON output generated: {json_path}")
        return json_path

    def _export_excel(self, output_path: Optional[str] = None) -> str:
        """Export results as Excel file"""
        base_path = output_path or self.output_file
        excel_path = base_path.replace('.csv', '.xlsx')

        df = self.get_results_dataframe()
        df.to_excel(excel_path, index=False)

        self.logger.info(f"Excel output generated: {excel_path}")
        return excel_path

    def clear_results(self) -> None:
        """Clear all stored results"""
        self.results.clear()
        self.logger.info("Results cleared")

    def get_result_count(self) -> int:
        """Get total number of results"""
        return len(self.results)
