-- The blend is part of a basket's content, and is frozen with the rest once the gate has run.
--
-- agb.basket_blend arrived after the pre-trade gate, and was made append-only but never given
-- the content_frozen trigger that agb.basket_rank, agb.basket_target and agb.basket_order
-- carry. Its rows could not be edited or deleted, but new ones could still be added to a gated
-- basket, which changes the coverage and spread the gate measured after it measured them.
-- The trigger refuses that insert, as it does on the other three, for every role.
--
-- Nothing legitimately writes a gated basket's blend: the pipeline inserts a basket's blend
-- when it proposes the basket, and a rerun copies the original's blend onto the new basket,
-- which has not been gated yet.

CREATE TRIGGER content_frozen BEFORE INSERT ON agb.basket_blend
    FOR EACH ROW EXECUTE FUNCTION agb.basket_content_check();
