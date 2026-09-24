"""The research profile: everything that trains, validates or scans a ranking model, on the
workstation only.

The live services never import this package. It reads a copy of the lake that `sync` has
pulled from the server (the workstation never pulls from the vendor) and builds the same
features, over the same universe, with the same cost model the live pipeline uses, so what
research measures is what the basket will trade.

    panel        the dataset: every month-end's features, each name's forward one-month return
                 and its quintile grade within that month
    train        LightGBM LambdaRank over that panel, one query group per month
    cpcv         combinatorial purged cross-validation with a one-month embargo
    walkforward  the annual expanding-window test: trained through year Y, trades Y+1
    scan         vectorbt scans of a signal, costed with the live cost model
    backtest     the NautilusTrader validation backtest: the same strategy, event-driven,
                 filled and charged by an independent engine, checking the scan
    gates        the validation gates on a model's CPCV paths: deflated Sharpe, probability
                 of backtest overfitting and cost-inclusive Sharpe, each at 1x modelled
                 cost, deflated Sharpe again at 2x, and the diagnostics beside them
    certify      one run of the gates over a configuration, the versioned artifact it leaves
                 and the import of its model_certification row; stage 1 only
    trials       the hashed ledger every configuration tried appends a row to
    sync         the read-only rsync of the server's lake

`python -m agb.research` runs each from the command line."""
