
# core python
from types import SimpleNamespace


class Transaction(SimpleNamespace):
    """ Facilitates object instance creation from dict """

    def __str__(self):
        return f"{self.TransactionCode} of {self.Quantity} units of {self.SecurityID1} in {self.PortfolioID} on {self.TradeDate}"
        


