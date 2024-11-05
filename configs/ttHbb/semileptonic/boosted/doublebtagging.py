def bbtagging(Jet, btag, wp, veto=False):
    if veto:
        return Jet[Jet[btag["bbtagging_algorithm"]] < btag["bbtagging_WP"][wp]]
    else:
        return Jet[Jet[btag["bbtagging_algorithm"]] > btag["bbtagging_WP"][wp]]
