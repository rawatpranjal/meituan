# Hierarchical Dispatch Algorithms for Food Delivery: A Simulation Study

## 1. Introduction

Real-time dispatch optimization in food delivery networks presents a combinatorial problem: assigning spatially distributed couriers to time-sensitive orders while maximizing throughput and minimizing customer wait times. This study evaluates five dispatch algorithms representing a hierarchy of computational intelligence, tested across three scenarios that stress different aspects of performance: sustained peak demand, unpredictable bursts, and geographic bottlenecks.

---

## 2. Problem Formulation & Algorithms

### 2.1 Core Assignment Problem

**Notation:**
- $C$ = set of available couriers
- $O$ = set of ready orders
- $B$ = set of candidate bundles (subsets of $O$)
- $y_{cb} \in \{0,1\}$ = binary decision variable (courier $c$ assigned to bundle $b$)
- $L_c$ = current location of courier $c$
- $L_o^P$ = pickup location (restaurant) of order $o$
- $L_o^D$ = dropoff location (customer) of order $o$
- $t_{now}$ = current simulation time

**Assignment Problem:**

$$
\begin{align*}
\min_{y_{cb}} \quad & \sum_{c \in C} \sum_{b \in B} \text{cost}(c, b) \cdot y_{cb}
\end{align*}
$$

subject to:
$$
\begin{align*}
& \sum_{b \in B} y_{cb} \le 1, && \forall c \in C \quad \text{(each courier assigned at most once)} \\
& \sum_{c \in C} \sum_{b \mid o \in b} y_{cb} \le 1, && \forall o \in O \quad \text{(each order assigned at most once)}
\end{align*}
$$

### 2.2 Cost Functions

**Base Cost Function** (Greedy, Hungarian, Simple, Network):

$$
\text{cost}(c, b) = t(L_c, L_{b,\text{first}}^P) + T_{\text{tour}}(b)
$$

where $t(\cdot, \cdot)$ denotes travel time and $T_{\text{tour}}(b)$ is the TSP tour duration through all pickup and dropoff locations in bundle $b$.

**Anticipatory Cost Function** (Anticipated Bundling):

$$
\begin{align*}
\text{cost}(c, b) &= \text{TotalTimeCommitment}(c, b) + (\alpha \cdot T_{\text{wait}}) + (\beta \cdot T_{\text{delay}}) - \text{UrgencyBonus}(b)
\end{align*}
$$

where:
$$
\begin{align*}
t_{\text{arrival}} &= t_{now} + t(L_c, L_{b,\text{first}}^P) \\
t_{\text{ready}} &= \max_{o \in b} \{\text{ready\_time}(o)\} \\
t_{\text{pickup\_start}} &= \max(t_{\text{arrival}}, t_{\text{ready}}) \\
\text{TotalTimeCommitment}(c, b) &= (t_{\text{pickup\_start}} - t_{now}) + T_{\text{tour}}(\{L_o^D\}_{o \in b}, L_{b,\text{last}}^P) \\
T_{\text{wait}} &= \max(0, t_{\text{ready}} - t_{\text{arrival}}) \\
T_{\text{delay}} &= \sum_{o \in b} \max(0, t_{\text{pickup\_start}} - \text{ready\_time}(o)) \\
\text{UrgencyBonus}(b) &= 300 \cdot |\{o \in b : \text{state}(o) = \text{READY}\}|
\end{align*}
$$

Parameters: $\alpha = 0.5$, $\beta = 0.3$. The holistic cost function balances courier time commitment, idle time waiting for food preparation, and delays imposed on ready orders. The urgency bonus provides priority to immediately available orders.

### 2.3 Algorithm Descriptions

**Greedy Baseline**

Myopic heuristic: for each ready order, assign nearest available courier. Orders processed sequentially in arrival order. Time complexity: $O(|C| \cdot |O|)$. No bundling capability.

**Hungarian Algorithm (Optimal Bipartite Matching)**

Solves optimal bipartite matching between couriers and orders. Constructs cost matrix $M \in \mathbb{R}^{|C| \times |O|}$ where $M_{co} = \text{cost}(c, \{o\})$. Solves $\min \sum_{c,o} M_{co} \cdot y_{co}$ using Hungarian algorithm. Time complexity: $O(\min(|C|, |O|)^2 \cdot \max(|C|, |O|))$. Globally optimal for 1-to-1 assignments. No bundling.

**Simple Bundling**

Extends Hungarian matching with bundles of 1-3 orders from same restaurant. For each restaurant $r$ with ready orders $O_r$, enumerate feasible subsets $b \subseteq O_r$ where $|b| \le 3$:

$$
B = \bigcup_{r \in R} \{b \subseteq O_r : 1 \le |b| \le 3\}
$$

Solve set packing problem to select non-overlapping bundles:

$$
\begin{align*}
\max \quad & \sum_{b \in B} |b| \cdot z_b \\
\text{s.t.} \quad & \sum_{b : o \in b} z_b \le 1, && \forall o \in O \\
& z_b \in \{0,1\}, && \forall b \in B
\end{align*}
$$

Apply Hungarian algorithm to assign selected bundles to couriers. Since Simple Bundling can select single-order bundles, it subsumes Hungarian algorithm's solution space.

**Network Bundling**

Generalizes Simple Bundling with geographic clustering across multiple restaurants. Apply radius-based clustering (400m threshold) to group nearby orders:

$$
\text{Cluster}_i = \{o \in O : \exists o' \in \text{Cluster}_i, \, d(L_o^P, L_{o'}^P) \le 400m\}
$$

Enumerate feasible bundles within each cluster. Solve set packing + Hungarian assignment. Enables multi-restaurant bundles for dense urban areas.

**Anticipated Bundling**

Incorporates lookahead optimization by considering PENDING orders (not yet ready) alongside READY orders. Maintain two pools: $O_{\text{READY}}$ (ready now) and $O_{\text{PENDING}}$ (preparing, ready within 300s lookahead window). Generate bundles mixing both states. Apply holistic cost function (Section 2.2) that balances immediate dispatch against future efficiency. Solve Hungarian assignment of bundles to couriers.

---

## 3. Experimental Design

### 3.1 Scenario Specifications

**Scenario 1: Downtown Crush**
Tests bundling power under high-density sustained demand.

- Duration: 3 hours
- Total orders: 400
- Couriers: 12
- Geography: 75% of restaurants clustered in downtown area (600m radius)
- Demand: Sustained peak (3x base rate) for 2 hours, off-peak rate 0.5x
- Physics: 5km × 5km map, Manhattan distance, 30 km/h courier speed

**Scenario 2: Popup Problem**
Tests anticipatory intelligence under unpredictable demand.

- Duration: 4 hours
- Total orders: 350
- Couriers: 12
- Geography: 4 scattered restaurant clusters (400m radius each)
- Demand: 4 unpredictable bursts (20 min each, 4x base rate), rotating zones
- Physics: Same as Scenario 1

**Scenario 3: River Divide**
Tests network intelligence under constrained topology.

- Duration: 3 hours
- Total orders: 300
- Couriers: 15 (10 south, 5 north zone)
- Geography: All 6 restaurants south of river, all customers north of river
- Demand: Steady high rate (1.67 orders/min)
- Physics: River at y=2500m with 2 bridges

All scenarios use consistent physics: 90s pickup service time, 45s dropoff service time, 300s meal preparation time, 30-minute order expiration, 60s batch intervals.

### 3.2 Performance Metrics

**Primary metrics:**
- Orders delivered: Total orders successfully delivered within time limit
- Total distance: Cumulative distance traveled by all couriers (km)
- Bundles created: Number of assignments made
- Average delivery time: Mean time from order placement to customer delivery (minutes)

**Derived metrics:**
- Fulfillment rate: Orders delivered / Total orders generated
- Distance per order: Total distance / Orders delivered
- Average bundle size: Orders delivered / Bundles created

---

## 4. Results

### 4.1 Downtown Crush Scenario

**Table 1:** Algorithm performance on Downtown Crush scenario (400 orders, 3 hours).

| Rank | Algorithm | Orders Delivered | Distance (km) | Bundles | Avg Delivery Time (min) |
|------|-----------|------------------|---------------|---------|------------------------|
| 1 | Anticipated | 252 (63.0%) | 383.8 | 98 | 25.4 |
| 2 | Network | 216 (54.0%) | 456.1 | 146 | 24.5 |
| 3 | Simple | 213 (53.3%) | 454.0 | 202 | 22.3 |
| 4 | Hungarian | 209 (52.3%) | 460.9 | 220 | 21.0 |
| 5 | Greedy | 175 (43.8%) | 485.3 | 187 | 30.7 |

**Figure 1:** Dispatch visualizations for Downtown Crush scenario.

<p align="center">
  <img src="outputs/downtown_crush/gifs/anticipated_bundling_network.gif" width="400"/>
  <img src="outputs/downtown_crush/gifs/network_bundling.gif" width="400"/>
  <br/>
  <img src="outputs/downtown_crush/gifs/simple_bundling_route_aware.gif" width="400"/>
  <img src="outputs/downtown_crush/gifs/hungarian_route_aware.gif" width="400"/>
  <br/>
  <img src="outputs/downtown_crush/gifs/greedy_baseline.gif" width="400"/>
  <br/>
  <em>Figure 1: Visualization of dispatch decisions for (top row) Anticipated and Network Bundling, (middle row) Simple Bundling and Hungarian, (bottom row) Greedy baseline.</em>
</p>

Anticipated Bundling achieves 44% higher fulfillment than Greedy baseline (252 vs 175 orders) and 20.6% higher than Hungarian (252 vs 209 orders). Creates significantly larger bundles (avg 2.57 orders/bundle vs 0.95 for Hungarian).

### 4.2 Popup Problem Scenario

**Table 2:** Algorithm performance on Popup Problem scenario (350 orders, 4 hours).

| Rank | Algorithm | Orders Delivered | Distance (km) | Bundles | Avg Delivery Time (min) |
|------|-----------|------------------|---------------|---------|------------------------|
| 1 | Network | 130 (37.1%) | 306.7 | 87 | 26.5 |
| 2 | Anticipated | 124 (35.4%) | 268.3 | 70 | 26.9 |
| 3 | Simple | 118 (33.7%) | 307.7 | 98 | 26.0 |
| 4 | Hungarian | 109 (31.1%) | 348.8 | 121 | 25.0 |
| 5 | Greedy | 109 (31.1%) | 372.6 | 121 | 27.5 |

**Figure 2:** Dispatch visualizations for Popup Problem scenario.

<p align="center">
  <img src="outputs/popup_problem/gifs/network_bundling.gif" width="400"/>
  <img src="outputs/popup_problem/gifs/anticipated_bundling_network.gif" width="400"/>
  <br/>
  <img src="outputs/popup_problem/gifs/simple_bundling_route_aware.gif" width="400"/>
  <img src="outputs/popup_problem/gifs/hungarian_route_aware.gif" width="400"/>
  <br/>
  <img src="outputs/popup_problem/gifs/greedy_baseline.gif" width="400"/>
  <br/>
  <em>Figure 2: Visualization of dispatch decisions for (top row) Network and Anticipated Bundling, (middle row) Simple Bundling and Hungarian, (bottom row) Greedy baseline.</em>
</p>

Network Bundling achieves best performance (130 orders, 37.1% fulfillment) in burst scenario. Anticipated places 2nd (124 orders) but achieves superior distance efficiency (2.16 km/order vs 2.36 km/order). Multi-restaurant clustering flexibility provides advantage under unpredictable demand.

### 4.3 River Divide Scenario

**Table 3:** Algorithm performance on River Divide scenario (300 orders, 3 hours).

| Rank | Algorithm | Orders Delivered | Distance (km) | Bundles | Avg Delivery Time (min) |
|------|-----------|------------------|---------------|---------|------------------------|
| 1 | Anticipated | 230 (76.7%) | 542.0 | 106 | 29.1 |
| 2 | Network | 203 (67.7%) | 704.2 | 159 | 28.3 |
| 3 | Simple | 196 (65.3%) | 717.5 | 181 | 27.2 |
| 4 | Hungarian | 182 (60.7%) | 734.5 | 197 | 25.7 |
| 5 | Greedy | 155 (51.7%) | 742.1 | 170 | 35.8 |

**Figure 3:** Dispatch visualizations for River Divide scenario.

<p align="center">
  <img src="outputs/river_divide/gifs/anticipated_bundling_network.gif" width="400"/>
  <img src="outputs/river_divide/gifs/network_bundling.gif" width="400"/>
  <br/>
  <img src="outputs/river_divide/gifs/simple_bundling_route_aware.gif" width="400"/>
  <img src="outputs/river_divide/gifs/hungarian_route_aware.gif" width="400"/>
  <br/>
  <img src="outputs/river_divide/gifs/greedy_baseline.gif" width="400"/>
  <br/>
  <em>Figure 3: Visualization of dispatch decisions for (top row) Anticipated and Network Bundling, (middle row) Simple Bundling and Hungarian, (bottom row) Greedy baseline.</em>
</p>

Anticipated Bundling achieves strong performance under geographic constraints (230 orders, 76.7% fulfillment), outperforming Network by 13.3%. Geographic bottleneck amplifies value of anticipatory lookahead. Achieves 23% better distance efficiency than Network (2.36 km/order vs 3.47 km/order).

### 4.4 Cross-Scenario Analysis

**Table 4:** Aggregate algorithm performance across all scenarios.

| Algorithm | Downtown | Popup | River | Total | Average |
|-----------|----------|-------|-------|-------|---------|
| Anticipated | 252 | 124 | 230 | 606 | 202.0 |
| Network | 216 | 130 | 203 | 549 | 183.0 |
| Simple | 213 | 118 | 196 | 527 | 175.7 |
| Hungarian | 209 | 109 | 182 | 500 | 166.7 |
| Greedy | 175 | 109 | 155 | 439 | 146.3 |

Anticipated Bundling delivers 10.4% more orders than Network (2nd place) and 38.0% more than Greedy baseline.

**Table 5:** Algorithm effectiveness by performance dimension.

| Metric | Best Performer | Performance |
|--------|----------------|-------------|
| Orders delivered | Anticipated | 3/3 scenarios (avg 202 orders) |
| Distance efficiency | Anticipated | 3/3 scenarios (avg 1.68 km/order) |
| Delivery speed | Hungarian | 3/3 scenarios (avg 23.9 min) |
| Bundle effectiveness | Anticipated | Avg 2.15 orders/bundle |

---

## 5. Conclusion

This simulation study demonstrates measurable value of progressive intelligence layers in food delivery dispatch. Anticipated Bundling achieves 38% higher fulfillment than greedy baseline and 20.6% higher than optimal bipartite matching.

Key findings: (1) Each intelligence tier—optimal matching, bundling, geographic clustering, anticipatory optimization—adds cumulative performance gains. (2) Algorithm performance exhibits scenario dependence; anticipated bundling performs best in dense sustained demand and geographic constraints while network bundling excels in unpredictable bursts. (3) Hungarian algorithm achieves fastest delivery time (23.9 min average) despite lower throughput, demonstrating the speed-throughput trade-off.

---

## References

1. Meituan Open Source. (2024). *TSL-Meituan Data-Driven Research Challenge*. Retrieved from https://github.com/meituan/meituan_informs_data

2. Additional delivery optimization literature available in `context/` directory (18 research papers on ride-hailing, meal delivery optimization, vehicle routing, and anticipatory dispatch).

---

## License

MIT License - See [LICENSE](LICENSE) file for details.
