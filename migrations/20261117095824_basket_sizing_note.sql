-- What the rebalance changed in a basket's sizing to fit the account's cash (issue #176).
--
-- Buys are whole shares of a twentieth of equity, so a basket can spend all its capital on
-- shares and leave nothing for the modelled cost of trading. The pre-trade gate refuses a
-- basket whose cash after modelled costs is negative, so `basket.propose` takes shares off the
-- lowest-ranked buys, one at a time, until the cash is not negative. The note says which buys
-- lost how many shares and what cost they paid for. It is null when nothing was trimmed, and
-- on every basket written before this column existed.
ALTER TABLE agb.basket ADD COLUMN sizing_note text CHECK (sizing_note <> '');
