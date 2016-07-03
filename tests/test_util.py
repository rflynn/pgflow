
import unittest

from pgflow.util import flatten


class TestFlatten(unittest.TestCase):

    def test_flatten_empty(self):
        self.assertEqual([], flatten([]))

    def test_flatten_empty_multilevel(self):
        self.assertEqual([], flatten([[[[[]]]]]))

    def test_flatten_already_flat(self):
        self.assertEqual([0], flatten([0]))

    def test_flatten_multilevel1(self):
        self.assertEqual([0], flatten([[0]]))

    def test_flatten_jagged(self):
        self.assertEqual([0,1,2], flatten([0,[1,[2]]]))
