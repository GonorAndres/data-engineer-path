"""Deduplication logic for streaming claims.

In a true streaming pipeline, the same claim_id can appear multiple times
within the same window (e.g., retries, duplicate publishes). This module
provides deduplication at the (claim_id, window) granularity.

On DirectRunner, BagState is supported but behaves synchronously.
The implementation uses BagState for portability across runners.
"""

from __future__ import annotations

import logging

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms.userstate import BagStateSpec

logger = logging.getLogger(__name__)


class DeduplicateClaims(beam.DoFn):
    """Remove duplicate claim_ids within the same window using BagState.

    Input:  (claim_id, claim_dict)  -- keyed by claim_id
    Output: claim_dict (only the first occurrence per key)

    Uses BagState to track seen claim_ids. Each new element checks the bag;
    if the claim_id is already present, the element is dropped.
    """

    SEEN_STATE = BagStateSpec("seen_ids", StrUtf8Coder())

    def process(self, element, seen=beam.DoFn.StateParam(SEEN_STATE)):
        claim_id, claim = element
        seen_ids = list(seen.read())

        if claim_id in seen_ids:
            logger.debug("Dropping duplicate claim_id=%s", claim_id)
            return

        seen.add(claim_id)
        yield claim
