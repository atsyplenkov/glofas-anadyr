---
layout: base.njk
title: About - GloFAS Anadyr
---

<div class="about-container">

# Reconstructing daily streamflow data for Anadyr River using GloFAS-ERA5 reanalysis

Repository contains code and data to reproduce the results of the paper "Reconstructing daily streamflow data for Anadyr River using GloFAS-ERA5 reanalysis" submitted to the journal "GEOGRAPHY, ENVIRONMENT, SUSTAINABILITY".

> *Tsyplenkov A., Shkolnyi D., Kravchenko A., Golovlev P.* Reconstructing daily streamflow data for Anadyr River using GloFAS-ERA5 reanalysis. GEOGRAPHY, ENVIRONMENT, SUSTAINABILITY. 2026 (*In Review*)

## Abstract

The Anadyr River is the largest river system in the Russian Far East with no water discharge observations available since 1996. The current study addresses this data scarcity by reconstructing daily streamflow series for the period 1979–2025 using the GloFAS-ERA5 v4.0 reanalysis product. To mitigate systematic model biases, we applied the Detrended Quantile Mapping correction method, optimised via a Leave-One-Out Cross-Validation strategy using historical gauging records and recent in-situ ADCP water discharge measurements.

The bias-correction procedure yielded a meaningful improvement in predictive performance, increasing the median Modified Kling-Gupta Efficiency by approximately 17% across the basin. Notably, the cross-validation analysis revealed that for stations previously used in initial global model calibration, a parsimonious linear scaling approach (with one quantile only) outperformed complex non-linear mapping, thereby preventing overfitting. The reconstructed long-term time series reveals a robust, statistically significant increasing trend in mean annual water discharge across the basin (up to 0.5% per year). These findings align the Anadyr River with the broader pattern of hydrological intensification observed across the Eurasian Arctic, likely driven by a shift in precipitation regimes from snow to rain during the shoulder seasons.

This research demonstrates that bias-corrected global reanalysis offers a reliable alternative to ground-based monitoring in data-scarce Arctic environments.

## Source Code

The full source code and data are available on [GitHub](https://github.com/atsyplenkov/glofas-anadyr).

</div>
