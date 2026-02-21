# Reconstructing daily streamflow data for Anadyr River using GloFAS-ERA5 reanalysis

Anatoly Tsyplenkov¹, Danila Shkolnyi¹, Arina Kravchenko¹, Pavel Golovlev¹

¹ *Lomonosov Moscow State University, Faculty of Geography, Moscow, Russian Federation*

**ABSTRACT**. The Anadyr River is the largest river system in the Russian Far East with no water discharge observations available since 1996. The current study addresses this data scarcity by reconstructing daily streamflow series for the period 1979–2025 using the GloFAS-ERA5 v4.0 reanalysis product. To mitigate systematic model biases, we applied the Detrended Quantile Mapping correction method, optimised via a Leave-One-Out Cross-Validation strategy using historical gauging records and recent in-situ ADCP water discharge measurements.

The bias-correction procedure yielded a meaningful improvement in predictive performance, increasing the median Modified Kling-Gupta Efficiency by approximately 17% across the basin. Notably, the cross-validation analysis revealed that for stations previously used in initial global model calibration, a parsimonious linear scaling approach (with one quantile only) outperformed complex non-linear mapping, thereby preventing overfitting. The reconstructed long-term time series reveals a robust, statistically significant increasing trend in mean annual water discharge across the basin (up to 0.5% per year). These findings align the Anadyr River with the broader pattern of hydrological intensification observed across the Eurasian Arctic, likely driven by a shift in precipitation regimes from snow to rain during the shoulder seasons. This research demonstrates that bias-corrected global reanalysis offers a reliable alternative to ground-based monitoring in data-scarce Arctic environments.

**KEYWORDS:** Anadyr River,  Arctic, Chukotka, GloFAS-ERA5 reanalysis, quantile mapping, bias correction, streamflow reconstruction

**ACKNOWLEDGEMENTS:** This work was financially supported by the Russian Science Foundation Project No. 24-27-00149.

**CONFLICT OF INTEREST:** The authors declare that they have no known competing financial interests or personal relationships that could have appeared to influence the work reported in this paper.

# 1. Introduction
The Anadyr River, flowing through the Chukotka Peninsula, is the largest river in Russia with no streamflow observations in the 21st century. The current network of hydrological stations within the basin has ceased providing information on water discharge since 1996. According to data from the Automated Information System of the State Monitoring of Water Bodies (AIS GMVO), as of early 2025, the Chukotka Administration for Hydrometeorology maintains only seven gauging stations in the Anadyr basin, all of which are limited to water stage measurements. This critical reduction in the hydrological observation network affects not only the Anadyr basin but the entirety of North-East Russia. Compared to the late 1980s, the number of gauging stations in this region has decreased by 67% [@shiklomanovWidespreadDeclineHydrological2002; @magritsky2022nddwrnrwfas; @tretiyakovStateRoshydrometHydrological2022]. Consequently, discharge observations are currently absent in the basins of the Chukchi and East Siberian Seas, east of the Kolyma River basin, and the Bering Sea, with the exception of a single discharge gauge on the Appanavayam River. Within the Russian Arctic zone, hydrometric coverage is currently limited to approximately 15% of the territory [@magritsky2022nddwrnrwfas].

Due to this severe data scarcity in the region, reliable estimates regarding streamflow trends remain virtually non-existent. Existing literature either implies that there have been no significant changes in long-term streamflow trends [@liGlobalTrendsWater2020] or that shifts occurred solely around 1970 [@frolova2022srrcfccrp1acwrrrod]. Nevertheless, the majority of studies indicate that Chukotka’s rivers are experiencing significant alterations in their water regime due to climate change [@magritsky2022nddwrnrwfas; @glotov2020ccrprwc]. This follows the general pattern of increasing discharge observed across the Eurasian Arctic [@liGlobalTrendsWater2020; @frolova2022srrcfccrp1acwrrrod; @tananaevTrendsAnnualExtreme2016]. A more detailed analysis, however, reveals spatial heterogeneity. The rivers of Western Chukotka, including parts of the Kolyma River basin draining into the East Siberian Sea, have demonstrated an increase in streamflow. This increase was most pronounced in the late twentieth century after 1980 [@glotov2020ccrprwc; @magritsky2022nddwrnrwfas]. Conversely, the streamflow of rivers draining into the Chukchi Sea decreased by 2.8% during the 1976–2017 period when compared to 1944–1975 [@glotov2020ccrprwc].

Alterations in streamflow with global warming are anticipated, as more detailed meteorological data have demonstrated shifts in precipitation regimes [@pendergrassUnevenNatureDaily2018]. This also includes the transition from snowfall to rainfall in May and September within mountainous areas, which significantly impacts runoff generation in the region [@makarieva2019wtaihrrrzcp; @makarieva2018wbhrmpwuskrrdkws1]. Yet, the question remains as to how and in what direction these changes have developed in the absence of continuous streamflow monitoring.

In data-scarce catchments, hydrological modelling offers a pathway to resolve these uncertainties [@hrachowitz2013dpubpr]. Global data reanalyses provide an excellent opportunity to bridge observational gaps by transferring knowledge from well-equipped regions to ungauged ones. The public release of the GloFAS-ERA5 v2.1 reanalysis [@harrigan2020gogrdr1p] generated considerable interest within the hydrological community through numerous applications for streamflow assessment globally [@winkelbauer2022derdaoiiovt; @zhao2024daelpgsr; @zhaoUnravellingPotentialGlobal2022; @senent-aparicio2021epgrdrdcsmgsmrbs; @chalov2022ovvmsnggaztok; @hunt2022ulsmlnnbrsfwus]. Since then, the GloFAS-ERA5 product has been updated frequently, with version v4.0 being the latest release available at the moment [@2025gv]. However, the application of 'raw' reanalysis products is prone to substantial error [@zhaoUnravellingPotentialGlobal2022; @ryazanovaBiascorrectedMonthlyPrecipitation2021]. In the case of GloFAS-ERA5, this typically leads to an underestimation of water discharge [@winkelbauer2022derdaoiiovt].

Therefore, as with any satellite-derived or model-based product, bias correction is required [@habib2014ebcserssubn]. The family of quantile mapping (QM) techniques is widely regarded as the standard approach to mitigate systematic bias. Quantile mapping remaps modelled values to align with the distribution of observed values and is a standard practice in meteorological and hydrological bias correction [@thrasherTechnicalNoteBias2012a]. Independent assessments for the contiguous United States have demonstrated significant improvements in GloFAS-ERA5 predictions following correction via the QM approach [@zhaoUnravellingPotentialGlobal2022].

In this regard, the primary objective of the current study is to provide an independent assessment of the GloFAS-ERA5 historical model regarding its capacity to estimate daily water discharge in an Arctic region, using the Anadyr River basin as an example. Using gauging station measurements from the late 20th century alongside mosaic in-situ observations, we tested different approaches for correcting reanalysis data and demonstrated their applicability in reconstructing water trends in data-scarce regions.

# 2. Study area
## 2.1 Hydrological data availability
The Anadyr River basin is characterised by a complex, dense river network and holds significant transport value for the settlements within its bounds. However, the gauging station network has been unable to provide information on the water discharge of the Anadyr River and its tributaries since the mid-1990s (Table 1, Figure 1). At present, no discharge gauging stations remain active within the basin.

A further challenge arises from the uneven distribution of the observation network. Water stage gauges are notably absent in the lower reaches of the river (Figure 1). Therefore, the river's water regime there can only be characterised based on the Utesiki station, which is located 185 km from the river mouth. Observations at this station stopped in 1993, and currently, no monitoring is conducted on the river reach from the estuary to the Ust-Belaya village (0 to 236 km). This is the most downstream station measuring water stage in the twenty-first century.

**Figure 1.** Map of the basin showing gauging stations and all toponyms mentioned in the text.

Hydrological studies of the river commenced in the 1950s with the establishment of a gauging station network and a permafrost station in Markovo. At the latter, sub-channel taliks were studied and classified for the first time based on their formation conditions, using the braided section of the Anadyr as a case study [@nekrasovTaliksRiverValleys1967]. To facilitate supply to the Otrozhny mine, a primary survey of the channel relief from the Krepost base to the estuary was conducted between 1968 and 1972. A more detailed resurvey was conducted in 1986, leading to an updated pilot chart containing a detailed river description [@amhanitskyPilotChartAnadyr1987]. Since 1990, research on the Anadyr River has been largely confined to its estuarine zone [@alexander2017material], with rare exceptions. Notably, two hydrological-geochemical surveys were conducted along the river's length in the mid-1990s, providing a comprehensive chemical characterisation of the water and bottom sediments [@huhFluvialGeochemistryRivers1998; @alexanderQuantificationNaturalBackgrounds1999]. The first detailed hydrological investigations of the braided section in the 21st century, which form the basis of the present work, were undertaken by the authors during 2020–2022 [@bakhareva2025sovremennye746874958].

**Table 1.** Gauging stations in the Anadyr River Basin with available daily streamflow data used in this study. See Figure 1 for their location.

| Gauging station ID | River    | Station       | Catchment area, km² | Start | End  |
| ------------------ | -------- | ------------- | ------------------- | ----- | ---- |
| 1496               | Anadyr   | Lamutskoe     | 16400               | 1978  | 1990 |
| 1497               | Anadyr   | Novyy Yeropol | 47300               | 1958  | 1996 |
| 1499               | Anadyr   | Snezhnoe      | 106000              | 1958  | 1993 |
| 1502               | Yeropol  | Chuvanskoe    | 7730                | 1978  | 1990 |
| 1504               | Mayn     | Vayegi        | 18600               | 1979  | 1990 |
| 1508               | Enmyvaam | Mukhomornoe   | 11400               | 1980  | 1994 |
| 1587               | Tanyurer | Tanyurer      | 18500               | 1979  | 1990 |

## 2.2 Anadyr River water regime
The formation of the water regime of the Anadyr River is determined by the geographical location of its basin, i.e. mountainous relief in the upper reaches and the continuous presence of permafrost. The water regime is also influenced by high lake density (2%) and the swampiness of the territory. These factors reduce peak discharges through the temporary accumulation of streamflow in local depressions on a floodplain and increase minimum low-water discharges due to the depletion of accumulated water volumes.

Three seasons can be identified in the regime of the Anadyr River: summer (June–August), late summer–autumn (August–September), and winter–spring (October–May). August, which includes the end of the freshet in its first ten days, is often complicated by rain floods. The freshet begins in late May or early June and ends in mid-July or early August (see Figure 2). Approximately 80% of the annual runoff occurs during the summer months. The shape of the hydrograph during the flood predominantly has a single peak, with the maximum discharge occurring in the second ten days of June, averaging 6899 m³/s (Anadyr – Snezhnoe, period 1958–1994).

**Figure 2.** Anadyr River Hydrograph at the Lamutskoe gauging station in 1986.

# 3. Materials and methods
## 3.1 Streamflow data
For this study, mean daily data for the 1958–1996 period from seven gauging stations located in the Anadyr River basin were used (see Figure 1 and Table 1). Water stage measurements at these stations occured at 8AM and 8PM (local time) or with increased frequency during high-flow periods [@izyurovaDevelopmentMethodsHydrometric2024]. Streamflow was further estimated based on rating curves, which are updated infrequently [@tretiyakovStateRoshydrometHydrological2022]. During the operating period of the streamflow gauges (prior to 1996) water discharge was measured using point velocity meters. For end-users, only mean daily water discharge ($Q$, m³/s) values are available, calculated as the average of the subdaily water discharge estimates.

In the current study, for the purpose of bias-correction of the GloFAS-ERA5 product, as was introduced generally in Section 1, we focused on the available overlapping period between modelled and observed daily streamflow (1979–1996). As evident from Figure 3, this period is poorly covered by daily streamflow data, with the longest available streamflow series being 17 years at the Anadyr – Novyy Yeropol station, and a minimum of 10 years at the Tanyurer – Tanyurer station. Even there, water discharge was not measured at all gauging stations during winter.

**Figure 3**. Daily streamflow data availability across the seven gauging stations in the Anadyr River basin for the 1979–1996 period. Each bar represents the percentage of missing or available data per year.

During the fieldwork campaigns of the 2021–2024 summers, we measured water discharge at several locations within the Anadyr River basin using an Acoustic Doppler Current Profiler (ADCP). Due to the braided nature of the channel system and the complex interchannel flow distribution, we targeted only sections of the Anadyr and Mayn Rivers where the majority of flow concentrated in a single channel. Cross-sectional measurements of flow intensity and velocity distribution were made using the SonTek RiverSurveyor M9. A compass and a GNSS receiver were used to orient the ADCP vertically and horizontally in space, providing full information about the position and direction of individual velocity vectors across the entire cross-section. For this purpose, a GNSS RTK base station was installed on the riverbank. To mitigate instrumental error, the final water discharge was estimated as the mean of three to four cross-sectional measurements.

## 3.2 GloFAS-ERA5
The GloFAS-ERA5 v4.0 discharge reanalysis is generated operationally based on the fifth generation of ECMWF atmospheric reanalysis (ERA5) for global precipitation, mean daily surface air temperature, relative humidity, incoming solar radiation, net longwave radiation, and mean wind speed [@harrigan2020gogrdr1p]. Specifically, it is produced by coupling surface and sub-surface runoff from the HTESSEL land surface model (Hydrology Tiled ECMWF Scheme for Surface Exchanges over Land) with the LISFLOOD v4.1.3 hydrological and channel routing model [@vanderknijffLISFLOODGISbasedDistributed2010; @harrigan2020gogrdr1p]. It covers the period from 1 January 1979 to near real-time, with a daily time step and a spatial resolution of 0.05° [@2025gv]. In this study, we used a specific sub-product of the GloFAS-ERA5 reanalysis — the mean water discharge (m³/s) in the last 24 hours. It is distributed as a three-dimensional NetCDF4 file with a grid cell area of ≈26.5 km² for the study region. Hereafter, the abbreviation GloFAS-ERA5 refers specifically to this product.

To calibrate the model underpinning the reanalysis, mean daily discharge data from approximately 2,500 gauging stations for the period 01.01.1980–31.12.2019 were used (https://confluence.ecmwf.int/display/CEMS/GloFAS+v4+calibration+data). According to official documentation, the median modified Kling-Gupta Efficiency ($KGE'$) in the fourth version of the reanalysis globally is 0.7, whereas in the second version, the median $KGE'$ was only 0.51 [@harrigan2020gogrdr1p]. In the Chukotka and Kamchatka territories, only three stations were used for calibration: Anadyr – Snezhnoe (observation record of 8–12 years), Penzhina – Kamenskoe (6–8 years), and Kamchatka – Ust-Kamchatsk (6–8 years). For all these gauging stations, the calibration $KGE'$ lies within the range of 0.9–1.0 (GloFAS-ERA5 v4.0).

## 3.3 Detrended Quantile Mapping
While numerous bias-correction approaches exist [@schmithIdentifyingRobustBias2021], our preliminary analysis indicated that Detrended Quantile Mapping (DQM) yields the most robust results. The core of every quantile mapping technique is the quantile-quantile relationship that maps the cumulative distribution function (CDF) of raw reanalysis data to the CDF of observed values [@woodLongrangeExperimentalHydrologic2002; @salehniaModellingReconstructingTree2022; @yuanExperimentalSeasonalHydrological2016].

DQM is one of two variants of quantile mapping introduced by @cannon2015bcgpqmhwmpcqe, designed to preserve the model's climate change signal while simultaneously bias-correcting the distribution of the target variable. Unlike simpler approaches, such as empirical quantile mapping, DQM can accommodate projected future values that fall outside the range of historical values [@burgerDownscalingExtremesIntercomparison2013]. DQM removes the long-term mean change from the model projections prior to quantile mapping, ensuring that input values lie within the range of the historical simulation values [@chadwick2023bapcvumgc]. The trend is then reimposed afterwards. In our case of GloFAS-ERA5 the correction is defined as follows:

$$Q_{cor}^{proj}(t) = F_{obs}^{-1}\left[F_{GloFAS}\left(Q_{GloFAS}^{proj}(t) \times \frac{\overline{Q_{GloFAS}^{hist}}} {\overline{Q_{GloFAS}^{proj}}} \right)\right] \times \frac{\overline{Q_{GloFAS}^{proj}}} {\overline{Q_{GloFAS}^{hist}}}$$

where $Q_{GloFAS}$ represents the daily streamflow values from the GloFAS-ERA5 dataset (m³/s), and the superscripts "hist" and "proj" denote "historical" and "projected" periods, respectively. $\overline{Q_{GloFAS}^{proj}}$ is the mean GloFAS-ERA5 water discharge for the period denoted in the superscript. The subscripts "obs" and "cor" signify "observed" and "bias-corrected" streamflow, respectively.

Studies typically use training periods spanning 10 to 40 years to ensure that internal variability does not become a dominant source of bias between the climate and the model [@maurerTechnicalNoteImpact2016]. In our data-scarce dataset, the maximum overlapping period between observed and GloFAS-ERA5 data is 17 years and the minimum is 10 years (cf. Figure 3). Therefore, we used Leave-One-Out Cross-Validation (LOOCV) for model quality assessment and hyperparameter tuning. Tuning was conducted by selecting the number of quantiles that yielded the best median performance across all folds. A detailed description of the approach follows in Section 3.4. Throughout the text, the original GloFAS-ERA5 data are referred to as "Raw", while the bias-corrected data have the "DQM" prefix.

## 3.4 Cross-validation strategy
Given the limited observational record (cf. Figure 3), splitting the data into static calibration and validation periods (Out-of-Sample evaluation) would result in insufficient training data to robustly estimate the bias correction transfer function. As noted by @bergmeirNoteValidityCrossvalidation2018, Cross-Validation (CV) allows for a more efficient use of data and can outperform OOS evaluation in time-series contexts. To account for the high temporal autocorrelation inherent in daily streamflow data, we applied a Leave-One-Out Cross-Validation (LOOCV) strategy. This approach preserves the intra-annual hydrograph structure and seasonal autocorrelation within the test sets while maximising the data available for training the quantile mapping function. We used this approach to independently evaluate the GloFAS-ERA5 performance in the Anadyr River. This procedure was also used to objectively select the optimal number of quantiles ($N_q$), preventing overfitting to specific events in the short historical record. Following the calibration strategy of the original GloFAS-ERA5 we selected the best $N_q$ based on the $KGE'$ metric.

## 3.5 Objective functions
Recent studies demonstrate that clear differences in optimal hydrographs can be detected when using different metrics for model assessment [@maiTenStrategiesSuccessful2023]. Following the recommendations from previous research [@maiTenStrategiesSuccessful2023; @clarkAbusePopularPerformance2021; @knoben2019tnibncnskges], we selected the following metrics for the GloFAS-ERA5 quality assessment and DQM tuning: $KGE'$, $NSE$, $NSE_{log}$, $RMSE$ and $pBIAS$. It has been shown that model calibration using Kling-Gupta Efficiency $KGE'$ [@klingRunoffConditionsUpper2012] typically yields better matching simulations during high-flow periods (e.g., April and May) compared to results obtained using Nash-Sutcliffe Efficiency $NSE$ [@nashRiverFlowForecasting1970] and root mean square error $RMSE$. Conversely, using the $NSE$ for log-transformed (i.e., $NSE_{log}$) discharge data places greater emphasis on low-flow periods in winter (December to March) and summer (July to October), while still achieving a reasonable magnitude for freshet high flows [@maiTenStrategiesSuccessful2023]. These metrics were estimated as follows using the 'hydroeval' Python library:

$$
NSE = 1 - \frac{\sum_{i=1}^{N}[e_{i}-s_{i}]^2}
{\sum_{i=1}^{N}[e_{i}-\mu(e)]^2}
$$

$$
KGE' = 1 - \sqrt{[\frac{\text{cov}(e, s)}{\sigma({e}) \cdot \sigma(s)} - 1]^2 + [\frac{\sigma(s) / \mu(s)}{\sigma(e) / \mu(e)} - 1]^2
+ [\frac{\mu(s)}{\mu(e)} - 1]^2}
$$


$$pBIAS = 100 \times \frac{\sum_{i=1}^{N}(e_{i}-s_{i})}{\sum_{i=1}^{N}e_{i}}$$


$$RMSE = \sqrt{\frac{1}{N}\sum_{i=1}^{N}[e_i-s_i]^2}$$

where $N$ is the length of the evaluation period; $e$ is the evaluation (observed) series; $s$ is the simulation series; $\mu$ is the arithmetic mean; $\sigma$ is the standard deviation; and $\text{cov}$ is the covariance. 

## 3.6 Statistical analysis
All statistical analyses and GIS procedures were performed in R version 4.5.1 using the ‘stats’, ‘sf’ [@pebesmaSfSimpleFeatures2016] packages, unless otherwise specified. Where possible the 95% credible intervals (95% CI) were reported in square brackets alongside with the median. Data retrieval and bias correction were performed in Python using the ‘xarray‘ and ‘xsdba‘ libraries. The code and data required to reproduce the results from the current study are available on Github (https://github.com/atsyplenkov/glofas-anadyr) and Zenodo archive.

The statistical significance of monotonic trends in water discharge was assessed using the non-parametric Mann-Kendall trend test [@mannNonparametricTestsTrend1945]. The magnitude of the trends was calculated using Sen’s slope estimator [@sen1968ercbkt]. Unlike parametric statistical tests, the Mann-Kendall test does not require data to be normally distributed. Additionally, as a rank-based method, it is resistant to the influence of outliers and small numbers of unusual values [@helsel2002smwr]. Sen’s slopes were further converted into percentage change per year by dividing them by the geometric mean of water discharge. The Mann-Kendall test was conducted in R using the ‘rkt’ package [@aldomarchettoRktMannKendallTest2012], as recommended by @helsel2020smwr.

# 4. Results

## 4.1 Model performance and bias-correction
The predictive performance of the GloFAS-ERA5 reanalysis varied considerably across gauging stations and cross-validation folds (Table 2). The median $KGE'$ values for the raw GloFAS-ERA5 data during the LOOCV ranged from 0.52 to 0.86. The highest median $KGE'$ was observed at the Anadyr – Snezhnoe gauging station (No. 1499), reaching 0.86 [95% CI 0.76 – 0.96]. Conversely, the lowest performance was recorded at the Enmyvaam – Mukhomornoe station (No. 1508), with a $KGE'$ of 0.52 [95% CI 0.44 – 0.73]. The $NSE$ metric demonstrated broad agreement with the $KGE'$ results, similarly identifying the best performance at station No. 1499 ($NSE$ = 0.86) and the poorest performance at the Yeropol – Chuvanskoe station (No. 1502), which yielded a median $NSE$ of 0.45. However, the log-transformed $NSE$ showed negative figures at some stations (1497 and 1499) and very low figures at others (for example, 1508).

The application of Detrended Quantile Mapping resulted in an improvement in performance metrics across the majority of stations (cf. Figure 4). The notable exception was station No. 1499, where the high initial performance remained largely stable (a slight decrease in $KGE'$ to 0.84 and a slight increase in $NSE$ to 0.87). On average, the bias-correction procedure yielded a performance increase estimated at +17.2% for $KGE'$ and +8.12% for $NSE$ values across all stations (see Figure 4). Additionally, the DQM improved the $NSE_{log}$ by increasing basin-wide median from 0.47 to 0.73.

The impact of bias correction on percentage bias ($pBIAS$) was heterogeneous. At certain stations (e.g., No. 1497 and 1504), the $pBIAS$ shifted from negative to positive values or vice versa while remaining within a similar absolute magnitude. However, substantial improvements were observed at other locations. Specifically, at stations No. 1508 and 1587, the DQM approach reduced the $pBIAS$ by approximately an order of magnitude (for example, from −16.8% to −0.08% at station No. 1508).

Using the optimal number of quantiles selected via LOOCV (see Table 2), we corrected the entire historical streamflow record. The corresponding scatter plot illustrating the relationship between observed and corrected GloFAS-ERA5 values is presented in Figure 5.

**Table 2.** Raw GloFAS-ERA5 and bias-corrected (DQM) model performance assessed during the leave-one-out cross-validation. Each metric column displays the median estimate alongside the range, estimated as the 95% credible interval. For the DQM, values are presented for the optimal number of quantiles ($N_q$).

| GaugeId | Type | KGE | NSE | NSElog | PBIAS | RMSE |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1496 | Raw | 0.57 [0.2 – 0.74] | 0.64 [0.27 – 0.73] | 0.41 [-0.19 – 0.77] | −2.18 [−32.5 – 10.5] | 290 [181 – 414] |
| 1496 | DQM | 0.75 [0.37 – 0.8] | 0.7 [0.22 – 0.83] | 0.75 [0.17 – 0.82] | 8.17 [−13 – 25.3] | 257 [183 – 379] |
| 1496 | $N_q$ | 20 | 10 | 50 | 5 | 10 |
| 1497 | Raw | 0.71 [0.15 – 0.87] | 0.76 [0.13 – 0.86] | -0.11 [−1.32 – 0.66] | −5.12 [−73.2 – 16.4] | 565 [315 – 959] |
| 1497 | DQM | 0.75 [0.41 – 0.88] | 0.75 [0.28 – 0.83] | 0.73 [0.21 – 0.91] | 7.31 [−51.1 – 31.6] | 573 [346 – 894] |
| 1497 | $N_q$ | 10 | 35 | 20 | 5 | 10 |
| 1499 | Raw | 0.86 [0.76 – 0.96] | 0.86 [0.76 – 0.96] | -0.29 [−2 – 0.82] | −2.43 [−19.8 – 17.3] | 629 [369 – 1 259] |
| 1499 | DQM | 0.84 [0.74 – 0.96] | 0.87 [0.69 – 0.96] | 0.85 [-0.27 – 0.94] | -0.22 [−85.8 – 23.5] | 652 [415 – 1 272] |
| 1499 | $N_q$ | 1 | 1 | 15 | 5 | 20 |
| 1502 | Raw | 0.58 [0.03 – 0.82] | 0.45 [−1.94 – 0.73] | 0.47 [-0.88 – 0.82] | 13.8 [−83.7 – 27.8] | 166 [109 – 246] |
| 1502 | DQM | 0.64 [-0.04 – 0.86] | 0.52 [−1.91 – 0.72] | 0.51 [-0.78 – 0.81] | 12.3 [−91.8 – 29.2] | 177 [108 – 242] |
| 1502 | $N_q$ | 15 | 1 | 35 | 25 | 5 |
| 1504 | Raw | 0.64 [0.27 – 0.77] | 0.64 [0.33 – 0.73] | 0.7 [−1.09 – 0.8] | 6.02 [−10 – 13.2] | 240 [196 – 517] |
| 1504 | DQM | 0.77 [0.38 – 0.85] | 0.75 [0.38 – 0.9] | 0.78 [0.6 – 0.87] | 8.13 [−5.78 – 17] | 199 [131 – 472] |
| 1504 | $N_q$ | 1 | 30 | 45 | 50 | 20 |
| 1508 | Raw | 0.52 [0.44 – 0.73] | 0.58 [0.43 – 0.7] | 0.28 [−3.77 – 0.69] | −16.8 [−44.2 – 34.4] | 173 [98.2 – 344] |
| 1508 | DQM | 0.75 [0.38 – 0.83] | 0.63 [0.23 – 0.75] | 0.75 [−2.28 – 0.91] | -0.08 [−19.5 – 49.1] | 155 [91 – 327] |
| 1508 | $N_q$ | 10 | 10 | 45 | 30 | 10 |
| 1587 | Raw | 0.61 [0.56 – 0.79] | 0.67 [0.43 – 0.86] | 0.66 [−1.7 – 0.8] | 15.8 [−16.7 – 31.2] | 343 [119 – 599] |
| 1587 | DQM | 0.68 [0.35 – 0.87] | 0.73 [0.19 – 0.91] | 0.67 [−2.02 – 0.86] | 3.39 [−37.5 – 24.4] | 270 [205 – 508] |
| 1587 | $N_q$ | 1 | 1 | 1 | 10 | 20 |

**Figure 4.** Estimated changes in median cross-validation metrics across all gauging stations between raw and bias-corrected GloFAS-ERA5 daily streamflow data for the Anadyr River basin.

**Figure 5.** Observed versus bias-corrected GloFAS-ERA5 daily discharge at gauging stations in the Anadyr River basin.

## 4.2 Streamflow trends in Anadyr River basin
The analysis of monotonic trends reveals a distinct contrast between the short, fragmented observational records and the long-term reconstructed time series (Table 3). Observational data, which are generally limited to the period prior to the early-1990s, show no statistically significant trends across any of the stations ($p > 0.05$). The direction of these non-significant trends varies, with negative slopes observed at stations 1496, 1497, 1499, and 1502, and positive slopes at stations 1504, 1508, and 1587 (see Figure 6).

**Figure 6.** Temporal variability of observed, raw and bias-corrected GloFAS-ERA5 mean annual water discharge at gauging stations in the Anadyr River basin. Straight lines shows Mann-Kendall linear trend.

In contrast, the reconstructed GloFAS-ERA5 series (spanning 1979–2025) indicates a consistent positive trend in mean annual water discharge across all analysed sub-basins. This increasing tendency is evident in both the raw and bias-corrected datasets. Notably, statistically significant increasing trends ($p < 0.05$) were identified at stations No. 1504 and No. 1587. For station No. 1504, the bias-corrected model estimates a significant increase of 0.5% per year ($p = 0.0018$), while station No. 1587 shows an increase of 0.49% per year ($p = 0.0365$). Although positive trends were observed at the remaining stations (ranging from 0.06% to 0.27% per year in the bias-corrected data), these did not reach statistical significance at the $\alpha = 0.05$ level. 

**Table 3.** Mann-Kendall (MK) monotonic trend estimates, expressed as % change per year and computed for mean annual water discharge values based on the observed, raw GloFAS-ERA5, and bias-corrected GloFAS-ERA5 datasets.

| GaugeId | Type | Period | MK Trend, %/yr | p-value |
| :--- | :--- | :--- | :--- | :--- |
| 1496 | Observed | 1978–1990 | -0.90 | 0.5371 |
| 1496 | Raw GloFAS-ERA5 | 1979–2025 | 0.22 | 0.2057 |
| 1496 | Bias-corrected GloFAS-ERA5 | 1979–2025 | 0.06 | 0.8689 |
| 1497 | Observed | 1958–1996 | -0.72 | 0.0628 |
| 1497 | Raw GloFAS-ERA5 | 1979–2025 | 0.23 | 0.2261 |
| 1497 | Bias-corrected GloFAS-ERA5 | 1979–2025 | 0.13 | 0.5696 |
| 1499 | Observed | 1958–1993 | -0.20 | 0.5321 |
| 1499 | Raw GloFAS-ERA5 | 1979–2025 | 0.27 | 0.0988 |
| 1499 | Bias-corrected GloFAS-ERA5 | 1979–2025 | 0.24 | 0.2405 |
| 1502 | Observed | 1978–1990 | −1.73 | 0.4507 |
| 1502 | Raw GloFAS-ERA5 | 1979–2025 | 0.23 | 0.3131 |
| 1502 | Bias-corrected GloFAS-ERA5 | 1979–2025 | 0.21 | 0.5329 |
| 1504 | Observed | 1979–1990 | 0.57 | 0.4363 |
| 1504 | Raw GloFAS-ERA5 | 1979–2025 | 0.40 | **0.0241** |
| 1504 | Bias-corrected GloFAS-ERA5 | 1979–2025 | 0.50 | **0.0018** |
| 1508 | Observed | 1980–1994 | 4.88 | 0.1926 |
| 1508 | Raw GloFAS-ERA5 | 1979–2025 | 0.33 | 0.1423 |
| 1508 | Bias-corrected GloFAS-ERA5 | 1979–2025 | 0.27 | 0.4858 |
| 1587 | Observed | 1979–1990 | 1.22 | 0.2105 |
| 1587 | Raw GloFAS-ERA5 | 1979–2025 | 0.47 | **0.0349** |
| 1587 | Bias-corrected GloFAS-ERA5 | 1979–2025 | 0.49 | **0.0365** |

# 5. Discussion

## 5.1 Model performance and bias correction
The application of Detrended Quantile Mapping to the GloFAS-ERA5 reanalysis yielded an improvement in performance metrics across the majority of the Anadyr River basin as evident from the LOOCV results and visual assessment of hydrographs (cf. Figure 2). This aligns with recent independent assessments of global hydrological models, which consistently highlight the necessity of post-processing to mitigate systematic biases inherent in reanalysis products. For instance, @hunt2022ulsmlnnbrsfwus demonstrated that while raw physics-based models provide a solid foundation, bias-correction techniques are essential for achieving operational forecast skill. Similarly, @zhaoUnravellingPotentialGlobal2022 reported that quantile mapping effectively corrects systematic errors in GloFAS-ERA5 streamflow values, although they noted that the efficacy of such corrections depends on the representativeness of the reference observational data.

Another interesting finding is that the evaluation of $NSE_{log}$ reveals a critical disparity in the raw reanalysis performance (at stations 1497 and 1499, see Table 2) where the raw model achieved a high standard $NSE$ of 0.76–0.86 but a negative $NSE_{log}$ of −0.11 and −0.29. Since $NSE_{log}$ emphasises low-flow performance [@maiTenStrategiesSuccessful2023], this difference may be attributed to structural biases in the underlying ERA5 forcing regarding cryospheric processes. @caoERA5LandSoilTemperature2020 reported that ERA5 and ERA5-Land show warm bias in soil temperature, resulting in an overestimation of active-layer thickness and a consequent underestimation of the near-surface permafrost area. In the context of the LISFLOOD hydrological model (backbone of the GloFAS-ERA5), this misrepresentation likely leads to erroneous baseflow recession curves during late summer and inaccurate discharge simulation in autumn, directly degrading the $NSE_{log}$ metric. The fact that DQM could successfully correct these values (see Table 2 and Figure 4) also suggests these low-flow errors are systematic, not random. According to these data, we can infer that raw reanalysis captures winter baseflow and summer recession temporal dynamics, the bias correction is neccesary for quantitatively accurate assessment.

One unanticipated result of our cross-validation procedure was the identification of a single quantile ($N_q = 1$) as the optimal hyperparameter for the Anadyr–Snezhnoe gauging station (No. 1499). In the context of the DQM algorithm, setting $N_q = 1$ effectively reduces the transfer function to a linear scaling approach (analogous to the 'Delta Change' method), wherein the distribution is adjusted by a constant factor rather than reshaping the entire cumulative distribution function. The Anadyr – Snezhnoe station was one of the few Arctic gauges used by @harrigan2020gogrdr1p for the calibration of the underlying LISFLOOD model in GloFAS-ERA5 v4.0. Consequently, the raw reanalysis data at this location already have a high correlation and distributional shape closely matching observations ($KGE' = 0.86$). This renders complex non-linear quantile adjustments redundant or potentially detrimental due to overfitting. Additionally, this validates the robustness of the proposed cross-validation strategy, which correctly identified that for a well-calibrated station, a parsimonious correction strategy is superior to a complex one. Conversely, for ungauged or poorly calibrated sub-basins, the preference for higher $N_q$ values indicates that the raw model failed to capture the specific distributional characteristics of the flow regime, necessitating a more aggressive non-linear correction.

Comparison of the bias-corrected streamflow with measured water discharges during the field campaigns of 2020–2024 showed that both raw and bias-corrected GloFAS-ERA5 data describe the modern streamflow conditions reasonably well (see Figure 7). The $KGE'$ for the raw data was 0.66, while for the bias-corrected data it was 0.77. Additionally, the $RMSE$ estimates decreased by 243 m³/s (17%) and $pBIAS$ decreased by 2%. While the general increase in predictive performance is evident here, one should treat these findings with caution. First, the amount of data points ($n = 8$) is too small to yield a robust conclusion, and more experiments are needed for a more statistically significant relationship. Secondly, the plot compares instantaneous water discharge (measured with ADCP) with mean daily reconstructed streamflow based on point velocity metres. Due to the diurnal cycle, the one-time measurement might not be representative. Additionally, a 2014 study by @bialik2014dmlrfceocfmeadcpa found that the difference in flow velocity obtained via ADCP and traditional metres lies in a 12–35% range depending on the number of verticals, and the latter tends to overestimate the velocity, and subsequently the water discharge.

That being said, we still believe that this relationship independently proves the quality of the GloFAS-ERA5 predictions and the effectiveness of DQM, adding some confidence in the streamflow trend estimates one can receive from GloFAS-ERA5 analysis.

**Figure 7.** Measured instantaneous ADCP water discharge versus raw and bias-corrected GloFAS-ERA5 daily discharge at the Anadyr – Snezhnoe in 2020–2024.

## 5.2 Streamflow trends and climatic drivers
The reconstruction of daily streamflow reveals a consistent positive trend in water discharge across the Anadyr basin over the 1979–2025 period. Crucially, the application of DQM largely preserved the direction and magnitude of the trends present in the raw reanalysis. Our estimated trends contribute new evidence to the ongoing debate regarding hydrological changes in the Eastern Arctic. While @magritsky2022nddwrnrwfas documented a general increase in freshwater flux into the Arctic Ocean, localised studies have presented a more complex picture. For example, @glotov2020ccrprwc observed that while rivers in Western Chukotka (in the Kolyma basin) exhibited significant flow increases, rivers draining into the Chukchi Sea displayed decreasing trends or no significant change during the late 20th century. Our results, showing statistically significant increases of approximately 0.50% per year at stations 1504 and 1587, suggest that the Anadyr basin is aligning more closely with the broader pattern of Arctic amplification and wetting observed in the Siberian sector [@magritsky2022nddwrnrwfas] rather than the drying trends previously reported for the Chukchi Sea basin.

The physical mechanism driving these positive trends is likely linked to alterations in the precipitation phase, a phenomenon observed in adjacent continuous permafrost regions. @makarieva2019wtaihrrrzcp analysed the Yana and Indigirka basins and identified a significant shift from solid to liquid precipitation during the shoulder seasons (May and September). They argued that approximately 10 mm of precipitation that historically fell as snow in early winter now falls as rain, contributing immediately to streamflow rather than being stored in the snowpack. It is highly probable that a similar mechanism is operative in the mountainous headwaters of the Anadyr River. A shift from snow to rain in May would accelerate the spring freshet and increase peak flows, while a similar shift in September would bolster autumn baseflow. This hypothesis is consistent with the observed increases in mean annual discharge and suggests that the hydrological regime of the Anadyr River is becoming increasingly dominated by rainfall-runoff processes.

# 6. Conclusion
This study addresses the critical hiatus in hydrological monitoring within the Anadyr River basin, the largest river system in the Russian Far East currently lacking operational discharge observations. By integrating fragmentary historical records with reanalysis data, we successfully reconstructed daily streamflow series for the period 1979–2025. Our evaluation demonstrates that while the raw GloFAS-ERA5 reanalysis provides a reasonable baseline, the application of bias correction is necessary for achieving operational accuracy in this complex cryolithozone environment. The quantile mapping procedure yielded a meaningful improvement in predictive performance, increasing the median Modified Kling-Gupta Efficiency by approximately 17% across the basin. Notably, our cross-validation strategy revealed that for stations previously used in the global model calibration, a simple linear scaling approach ($N_q=1$) is preferable to complex non-linear mapping, thereby preventing overfitting.

The reconstructed long-term time series reveals a robust and statistically significant increasing trend in mean annual water discharge across the basin, a signal that was previously obscured by the brevity and discontinuity of the observational record. This finding aligns the Anadyr River with the broader pattern of hydrological intensification observed across the Eurasian Arctic, likely driven by a shift in precipitation regimes from snow to rain during the shoulder seasons. Furthermore, the identified positive trends in freshwater flux have profound implications for the sediment dynamics, thermal regime, and estuarine ecology of the region, necessitating updated strategies for water resource management and infrastructure planning in Chukotka.

# References
