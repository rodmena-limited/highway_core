import pytest
from highway_core.engine.operator_handlers.base_handler import BaseOperatorHandler


def test_base_operator_handler_abstract_methods():
    """Test that BaseOperatorHandler is abstract and cannot be instantiated directly"""
    # Should not be able to instantiate an abstract class directly
    with pytest.raises(TypeError):
        BaseOperatorHandler()
