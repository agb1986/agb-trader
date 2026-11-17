-- One accepted rerun per missed basket, and one per rerun reference.
--
-- `agb rerun` read the missed basket, checked that no rerun of it was made or asked for, chose
-- the next free reference of the period and recorded the accepted command, all without a lock.
-- Two requests at once both passed the check, and both recorded a rerun: two baskets built
-- from one missed rebalance, which would trade it twice, under one reference or under two.
--
-- These partial unique indexes make the second accepted command fail to insert. The CLI
-- catches the violation and records the request as refused, with the rerun that won named as
-- the cause. A refused command is not covered, so every refusal is still kept.
--
-- An accepted rerun stays accepted after its basket is built (the view agb.rerun_pending only
-- stops listing it), and a basket is rerun once: a rerun that is missed in turn is a basket of
-- its own, and is rerun under its own id.

CREATE UNIQUE INDEX command_rerun_basket ON agb.command ((args->>'basket_id'))
    WHERE verb = 'rerun' AND outcome = 'accepted';

CREATE UNIQUE INDEX command_rerun_reference ON agb.command ((args->>'rerun'))
    WHERE verb = 'rerun' AND outcome = 'accepted';
