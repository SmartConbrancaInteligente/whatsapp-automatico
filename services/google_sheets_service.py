from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import gspread


class GoogleSheetsClient:
    def __init__(self, spreadsheet_id: str, worksheet_name: str, service_account_file: str) -> None:
        self.spreadsheet_id = spreadsheet_id
        self.worksheet_name = worksheet_name
        self.service_account_file = service_account_file

    @property
    def enabled(self) -> bool:
        return bool(
            self.spreadsheet_id.strip()
            and self.worksheet_name.strip()
            and self.service_account_file.strip()
        )

    def _worksheet(self):
        client = gspread.service_account(filename=self.service_account_file)
        spreadsheet = client.open_by_key(self.spreadsheet_id)
        return spreadsheet.worksheet(self.worksheet_name)

    def find_row_by_number(self, number: str) -> Optional[int]:
        if not self.enabled:
            return None

        worksheet = self._worksheet()
        values = worksheet.get_all_values()
        if not values:
            return None

        header = values[0]
        if "numero" not in header:
            return None

        number_col = header.index("numero")
        for row_index, row in enumerate(values[1:], start=2):
            if len(row) > number_col and str(row[number_col]).strip() == str(number).strip():
                return row_index

        return None

    def update_due_date_by_number(self, number: str, new_due_date: str) -> bool:
        if not self.enabled:
            return False

        worksheet = self._worksheet()
        values = worksheet.get_all_values()
        if not values:
            return False

        header = values[0]
        if "numero" not in header or "vencimento" not in header:
            return False

        number_col = header.index("numero") + 1
        due_col = header.index("vencimento") + 1

        target_row = None
        for row_index, row in enumerate(values[1:], start=2):
            if len(row) >= number_col and str(row[number_col - 1]).strip() == str(number).strip():
                target_row = row_index
                break

        if not target_row:
            return False

        worksheet.update_cell(target_row, due_col, new_due_date)
        return True

    @staticmethod
    def add_one_month(date_str: str) -> Optional[str]:
        try:
            current = datetime.strptime(date_str.strip(), "%d/%m/%Y")
        except ValueError:
            return None

        month = current.month + 1
        year = current.year
        if month > 12:
            month = 1
            year += 1

        day = min(current.day, GoogleSheetsClient._days_in_month(year, month))
        updated = current.replace(year=year, month=month, day=day)
        return updated.strftime("%d/%m/%Y")

    @staticmethod
    def _days_in_month(year: int, month: int) -> int:
        if month == 2:
            is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
            return 29 if is_leap else 28
        if month in {4, 6, 9, 11}:
            return 30
        return 31
