"""
Workflow node base classes and implementations.

This module provides the base classes for creating workflow nodes
that integrate with the PocketFlow framework.
"""

from .base_node import WorkflowNode
from .nifi_node import NiFiWorkflowNode

__all__ = ['WorkflowNode', 'NiFiWorkflowNode'] 