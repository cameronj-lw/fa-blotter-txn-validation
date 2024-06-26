
# core python
import logging

# pypi
import sqlalchemy
from sqlalchemy import sql

# native
from infrastructure.util.table import BaseTable, ScenarioTable



""" LWDB """

class LWDBNotificationTable(ScenarioTable):
	config_section = 'lwdb'
	table_name = 'notification'

	def read(self, scenario=None, status=None, data_date=None, trade_date=None, portfolio_code=None, lw_id=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		stmt = stmt.where(self.c.scenario == (scenario or self.base_scenario))
		if status is not None:
			stmt = stmt.where(self.c.status == status)
		if data_date is not None:
			stmt = stmt.where(self.c.data_dt == data_date)
		if trade_date is not None:
			stmt = stmt.where(self.c.TradeDate == trade_date)
		if portfolio_code is not None:
			stmt = stmt.where(self.c.PortfolioCode == portfolio_code)
		if lw_id is not None:
			stmt = stmt.where(self.c.ProprietarySymbol == lw_id)
		return self.execute_read(stmt)



""" APXDB """

class APXDBvPortfolioTransactionView(BaseTable):
	config_section = 'apxdb'
	schema = 'AdvApp'
	table_name = 'vPortfolioTransaction'

	def read(self, trade_date=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if trade_date is not None:
			stmt = stmt.where(self.c.TradeDate == trade_date)
		return self.execute_read(stmt)

class APXDBvPortfolioTransactionLWFundsView(BaseTable):
	config_section = 'apxdb_lwp'
	table_name = 'vPortfolioTransaction_LW_Funds'

	def read(self, trade_date=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if trade_date is not None:
			stmt = stmt.where(self.c.TradeDate == trade_date)
		return self.execute_read(stmt)



""" MGMTDB """

class MGMTDBMonitorTable(ScenarioTable):
	config_section = 'mgmtdb'
	table_name = 'monitor'

	def read(self, scenario=None, data_date=None, run_group=None, run_name=None, run_type=None, run_host=None, run_status_text=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if scenario is not None:
			stmt = stmt.where(self.c.scenario == scenario)
		if data_date is not None:
			stmt = stmt.where(self.c.data_dt == data_date)
		if run_group is not None:
			stmt = stmt.where(self.c.run_group == run_group)
		if run_name is not None:
			stmt = stmt.where(self.c.run_name == run_name)
		if run_type is not None:
			stmt = stmt.where(self.c.run_type == run_type)
		if run_host is not None:
			stmt = stmt.where(self.c.run_host == run_host)
		if run_status_text is not None:
			stmt = stmt.where(self.c.run_status_text == run_status_text)
		return self.execute_read(stmt)

	def read_for_date(self, data_date):
		"""
		Read all entries for a specific date

		:param data_date: The data date
		:returns: DataFrame
		"""
		stmt = (
			sql.select(self.table_def)
			.where(self.c.data_dt == data_date)
		)
		data = self.execute_read(stmt)
		return data



