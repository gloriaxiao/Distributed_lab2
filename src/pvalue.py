#!/usr/bin/env python


class Pvalue:
	def __init__(self, ballot_num, slot_num, c):
		self.ballot = ballot_num
		self.slot = slot_num
		self.command = c

	def __str__(self):
		return "{:d} {:d} {}".format(self.ballot, self.slot, self.command)
