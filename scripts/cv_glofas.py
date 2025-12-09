import hydroeval as he
import numpy as np

simulations = [5.3, 4.2, 5.7, 2.3]
evaluations = [4.7, 4.3, 5.5, 2.7]

nse = he.evaluator(he.nse, simulations, evaluations)
kge, r, alpha, beta = he.evaluator(he.kgeprime, simulations, evaluations)
kgenp, r, alpha, beta = he.evaluator(he.kgenp, simulations, evaluations)
pbias = he.evaluator(he.pbias, simulations, evaluations)

print(nse, kge, kgenp, pbias)