-- The lake gains a fourth dataset, SFP, which holds SPY's daily prices: the benchmark the
-- certification's excess-return gates measure against. SEP is equities only, so without SFP
-- the lake had no SPY return and those gates were undefined. Each SFP partition is recorded in
-- the lake snapshot ledger like any other, so the ledger's dataset list takes 'sfp'.
ALTER TABLE agb.lake_snapshot DROP CONSTRAINT lake_snapshot_dataset_check;
ALTER TABLE agb.lake_snapshot ADD CONSTRAINT lake_snapshot_dataset_check
    CHECK (dataset IN ('sep', 'sf1', 'tickers', 'sfp'));
