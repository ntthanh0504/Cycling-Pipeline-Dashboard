from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

class Expectation(ABC):
    """
    Abstract base class for defining expectations on data columns.
    """

    def __init__(self, column: str, dimension: str, add_info: Optional[Dict[str, Any]] = None):
        """
        Initialize the Expectation with a column, dimension, and additional information.

        :param column: The name of the column to apply the expectation to.
        :param dimension: The dimension of the expectation.
        :param add_info: Additional information for the expectation.
        """
        self.column = column
        self.dimension = dimension
        self.add_info = add_info if add_info is not None else {}

    @abstractmethod
    def test(self, ge_df: Any) -> bool:
        """
        Abstract method to test the expectation on a given dataframe.

        :param ge_df: The dataframe to test the expectation on.
        :return: True if the expectation is met, False otherwise.
        """
        pass