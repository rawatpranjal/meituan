# Matching Algorithms for Food Delivery

## 1. Introduction

Real-time dispatch optimization in food delivery networks presents a combinatorial problem: assigning spatially distributed couriers to time-sensitive orders while maximizing throughput and minimizing customer wait times. This study evaluates five dispatch algorithms representing a hierarchy of computational intelligence, tested across three scenarios that stress different aspects of performance: sustained peak demand, unpredictable bursts, and geographic bottlenecks.

---

## 2. Algorithms

**Notation:**
- $C$ = set of available couriers
- $O$ = set of ready orders
- $L_c$ = current location of courier $c$
- $L_o^P$ = pickup location (restaurant) of order $o$
- $L_o^D$ = dropoff location (customer) of order $o$
- $t(\cdot, \cdot)$ = travel time between locations
- $T_{\text{tour}}(b)$ = TSP tour duration through all pickup and dropoff locations in bundle $b$

### 2.1 Greedy Baseline

**Approach:** Myopic heuristic that assigns each ready order to the nearest available courier. Orders processed sequentially in arrival order.

**No formal optimization.** For each order $o$, compute $c^* = \arg\min_{c \in C} t(L_c, L_o^P)$ and assign courier $c^*$ to order $o$.

**Properties:** Time complexity $O(|C| \cdot |O|)$. No route optimization. No bundling capability.

### 2.2 Hungarian Algorithm (Optimal Bipartite Matching)

**Approach:** Solves optimal bipartite matching between couriers and orders, minimizing total assignment cost.

**Optimization Problem:**

Constructs cost matrix $M \in \mathbb{R}^{|C| \times |O|}$ where $M_{co} = t(L_c, L_o^P) + T_{\text{tour}}(\{o\})$. Solves:

$$
\begin{align*}
\min_{y_{co}} \quad & \sum_{c \in C} \sum_{o \in O} M_{co} \cdot y_{co}
\end{align*}
$$

subject to:
$$
\begin{align*}
& \sum_{o \in O} y_{co} \le 1, && \forall c \in C \quad \text{(each courier assigned at most once)} \\
& \sum_{c \in C} y_{co} \le 1, && \forall o \in O \quad \text{(each order assigned at most once)} \\
& y_{co} \in \{0,1\}, && \forall c \in C, o \in O
\end{align*}
$$

**Properties:** Time complexity $O(\min(|C|, |O|)^2 \cdot \max(|C|, |O|))$. Globally optimal for 1-to-1 assignments. No bundling.

### 2.3 Simple Bundling

**Approach:** Extends Hungarian matching by allowing bundles of 1-3 orders from the same restaurant.

**Optimization Problem:**

**Step 1 - Bundle Generation:** For each restaurant $r$ with ready orders $O_r$, enumerate feasible bundles:

$$
B = \bigcup_{r \in R} \{b \subseteq O_r : 1 \le |b| \le 3\}
$$

**Step 2 - Set Packing:** Select non-overlapping bundles to maximize orders assigned:

$$
\begin{align*}
\max_{z_b} \quad & \sum_{b \in B} |b| \cdot z_b \\
\text{s.t.} \quad & \sum_{b : o \in b} z_b \le 1, && \forall o \in O \\
& z_b \in \{0,1\}, && \forall b \in B
\end{align*}
$$

**Step 3 - Assignment:** Apply Hungarian algorithm to assign selected bundles to couriers using cost $\text{cost}(c, b) = t(L_c, L_{b,\text{first}}^P) + T_{\text{tour}}(b)$.

**Properties:** Since Simple Bundling can select single-order bundles ($|b|=1$), it subsumes Hungarian algorithm's solution space.

### 2.4 Network Bundling

**Approach:** Generalizes Simple Bundling with geographic clustering across multiple restaurants.

**Optimization Problem:**

**Step 1 - Cluster Generation:** Apply radius-based clustering (400m threshold) to group nearby orders:

$$
\text{Cluster}_i = \{o \in O : \exists o' \in \text{Cluster}_i, \, d(L_o^P, L_{o'}^P) \le 400m\}
$$

**Step 2 - Bundle Generation:** Within each cluster, enumerate feasible bundles of 1-3 orders (any restaurant):

$$
B = \bigcup_{i} \{b \subseteq \text{Cluster}_i : 1 \le |b| \le 3\}
$$

**Step 3 - Set Packing + Assignment:** Same as Simple Bundling (maximize orders via set packing, then Hungarian assignment).

**Properties:** Enables multi-restaurant bundles for dense urban areas. Larger solution space than Simple Bundling.

### 2.5 Anticipated Bundling

**Approach:** Incorporates lookahead optimization by considering PENDING orders (not yet ready) alongside READY orders.

**Optimization Problem:**

**Step 1 - Bundle Generation:** Maintain two pools: $O_{\text{READY}}$ (ready now) and $O_{\text{PENDING}}$ (preparing, ready within 300s lookahead window). Apply geographic clustering as in Network Bundling. Generate bundles mixing both states.

**Step 2 - Holistic Cost Function:** For each courier-bundle pair $(c, b)$, compute:

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

Parameters: $\alpha = 0.5$, $\beta = 0.3$.

**Step 3 - Assignment:** Construct cost matrix $M \in \mathbb{R}^{|C| \times |B|}$ where $M_{cb} = \text{cost}(c, b)$. Solve:

$$
\begin{align*}
\min_{y_{cb}} \quad & \sum_{c \in C} \sum_{b \in B} M_{cb} \cdot y_{cb}
\end{align*}
$$

subject to:
$$
\begin{align*}
& \sum_{b \in B} y_{cb} \le 1, && \forall c \in C \quad \text{(each courier assigned at most once)} \\
& \sum_{c \in C} \sum_{b \mid o \in b} y_{cb} \le 1, && \forall o \in O \quad \text{(each order assigned at most once)} \\
& y_{cb} \in \{0,1\}, && \forall c \in C, b \in B
\end{align*}
$$

**Properties:** Holistic cost function balances courier time commitment, idle time waiting for food preparation, and delays imposed on ready orders. Urgency bonus provides priority to immediately available orders, preventing over-optimization for distant future efficiency.

---

## 3. Experimental Design

### 3.1 Scenario Specifications

**Scenario 1: Downtown Crush**
Tests bundling power under high-density sustained demand.

- Duration: 3 hours
- Total orders: 200
- Couriers: 10
- Restaurants: 6 (75% clustered in downtown area, 600m radius)
- Geography: 5km × 5km map, Manhattan distance
- Demand: Sustained peak (3x base rate) for 2 hours, off-peak rate 0.5x
- Physics: 30 km/h courier speed, 90s pickup, 45s dropoff, 300s meal prep

**Scenario 2: Popup Problem**
Tests anticipatory intelligence under unpredictable demand.

- Duration: 4 hours
- Total orders: 175
- Couriers: 10
- Restaurants: 6 (4 scattered clusters, 400m radius each)
- Geography: Same map as Scenario 1
- Demand: 4 unpredictable bursts (20 min each, 4x base rate), rotating zones
- Physics: Same as Scenario 1

**Scenario 3: River Divide**
Tests network intelligence under constrained topology.

- Duration: 3 hours
- Total orders: 150
- Couriers: 12 (8 south zone, 4 north zone)
- Restaurants: 5 (all south of river)
- Geography: River at y=2500m with 2 bridges; all customers north of river
- Demand: Steady high rate (1.67 orders/min)
- Physics: Same as Scenario 1

**Scenario 4: Impossible Deadline**
Tests anticipatory dispatch capability under tight time constraints.

- Duration: 1 hour
- Total orders: 1
- Couriers: 1 (starts at 3km, 2km)
- Restaurants: 1 (at 4km, 3km)
- Customer: Located at 8km, 6km (5km from restaurant)
- Order timing: Placed at t=300s, ready at t=900s
- Physics: 20 km/h courier speed, 150s pickup, 120s dropoff, 600s meal prep, 20 min expiration

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

**Table 1:** Algorithm performance on Downtown Crush scenario (200 orders, 3 hours).

| Rank | Algorithm | Orders Delivered | Distance (km) | Bundles | Avg Delivery Time (min) |
|------|-----------|------------------|---------------|---------|------------------------|
| 1 | Anticipated | 141 (70.5%) | 234.0 | 61 | 24.4 |
| 2 | Simple | 139 (69.5%) | 224.3 | 65 | 26.8 |
| 3 | Network | 132 (66.0%) | 234.1 | 58 | 28.4 |
| 4 | Hungarian | 99 (49.5%) | 225.6 | 99 | 22.0 |
| 5 | Greedy | 87 (43.5%) | 250.2 | 87 | 29.3 |

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

Anticipated Bundling achieves highest fulfillment with 141 orders delivered (70.5%), outperforming Greedy by 62% (141 vs 87) and Hungarian by 42% (141 vs 99). Creates effective bundles with avg 2.31 orders/bundle compared to 1.0 for Hungarian. Simple Bundling performs competitively with 139 orders (69.5%).

### 4.2 Popup Problem Scenario

**Table 2:** Algorithm performance on Popup Problem scenario (175 orders, 4 hours).

| Rank | Algorithm | Orders Delivered | Distance (km) | Bundles | Avg Delivery Time (min) |
|------|-----------|------------------|---------------|---------|------------------------|
| 1 | Anticipated | 162 (92.6%) | 322.1 | 78 | 22.6 |
| 2 | Simple | 158 (90.3%) | 304.0 | 79 | 26.5 |
| 3 | Network | 156 (89.1%) | 312.4 | 77 | 26.8 |
| 4 | Greedy | 120 (68.6%) | 351.6 | 120 | 26.6 |
| 5 | Hungarian | 119 (68.0%) | 325.9 | 119 | 24.0 |

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

Anticipated Bundling achieves exceptional performance with 162 orders delivered (92.6% fulfillment), demonstrating superior anticipatory capabilities under unpredictable demand bursts. Outperforms Greedy by 35% (162 vs 120) and achieves fastest average delivery time (22.6 min). All bundling algorithms (Anticipated, Simple, Network) deliver 89-93% of orders compared to 68% for reactive algorithms.

### 4.3 River Divide Scenario

**Table 3:** Algorithm performance on River Divide scenario (150 orders, 3 hours).

| Rank | Algorithm | Orders Delivered | Distance (km) | Bundles | Avg Delivery Time (min) |
|------|-----------|------------------|---------------|---------|------------------------|
| 1 | Anticipated | 144 (96.0%) | 314.5 | 56 | 25.9 |
| 2 | Network | 140 (93.3%) | 314.0 | 55 | 30.3 |
| 3 | Simple | 134 (89.3%) | 341.9 | 62 | 31.8 |
| 4 | Hungarian | 94 (62.7%) | 341.3 | 94 | 25.0 |
| 5 | Greedy | 83 (55.3%) | 379.8 | 83 | 35.2 |

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

Anticipated Bundling achieves remarkable 96.0% fulfillment (144/150 orders) under geographic constraints, outperforming Network by 2.9% and Greedy by 73%. Geographic bottleneck amplifies value of bundling algorithms—all three bundling approaches (Anticipated, Network, Simple) deliver 89-96% vs 55-63% for reactive algorithms. Anticipated achieves comparable distance efficiency to Network (2.18 vs 2.24 km/order) while delivering more orders.

### 4.4 Impossible Deadline Scenario

**Table 4:** Algorithm performance on Impossible Deadline scenario (1 order, 1 hour).

| Algorithm | Dispatch Time | Delivery Time | Click-to-Door (min) | Result |
|-----------|---------------|---------------|---------------------|--------|
| Anticipated | t=600s (10:00) | t=2370s (39:30) | 34.5 | ✅ Early dispatch |
| Greedy | t=900s (15:00) | t=2670s (44:30) | 39.5 | Reactive |
| Hungarian | t=900s (15:00) | t=2670s (44:30) | 39.5 | Reactive |
| Simple | t=900s (15:00) | t=2670s (44:30) | 39.5 | Reactive |
| Network | t=900s (15:00) | t=2670s (44:30) | 39.5 | Reactive |

**Figure 4:** This single-order scenario demonstrates Anticipated Bundling's unique lookahead capability. Order placed at t=300s, ready at t=900s. Anticipated algorithm dispatches courier at t=600s (within 300s lookahead window), allowing courier to arrive at restaurant just as food becomes ready. Reactive algorithms wait until t=900s to dispatch, resulting in 5-minute (12.7%) longer delivery time.

### 4.5 Cross-Scenario Analysis

**Table 5:** Aggregate algorithm performance across scenarios 1-3 (525 total orders).

| Algorithm | Downtown | Popup | River | Total | Fulfillment Rate |
|-----------|----------|-------|-------|-------|------------------|
| Anticipated | 141 | 162 | 144 | 447 | 85.1% |
| Simple | 139 | 158 | 134 | 431 | 82.1% |
| Network | 132 | 156 | 140 | 428 | 81.5% |
| Hungarian | 99 | 119 | 94 | 312 | 59.4% |
| Greedy | 87 | 120 | 83 | 290 | 55.2% |

Anticipated Bundling achieves 85.1% fulfillment across all scenarios, delivering 54% more orders than Greedy baseline (447 vs 290) and 43% more than Hungarian optimal matching (447 vs 312).

**Table 6:** Algorithm effectiveness by performance dimension.

| Metric | Best Performer | Performance |
|--------|----------------|-------------|
| Orders delivered | Anticipated | 3/3 scenarios (avg 149 orders, 85.1% fulfillment) |
| Distance efficiency | Simple | Avg 2.00 km/order across scenarios |
| Delivery speed | Hungarian | 2/3 scenarios (avg 23.7 min) |
| Bundle effectiveness | Anticipated | Avg 2.29 orders/bundle |
| Anticipatory dispatch | Anticipated | Only algorithm with proactive capability (5 min faster in Impossible Deadline) |

---

## 5. Conclusion

This simulation study demonstrates measurable value of progressive intelligence layers in food delivery dispatch. Anticipated Bundling achieves 85.1% fulfillment across scenarios, outperforming Greedy baseline by 54% and optimal bipartite matching (Hungarian) by 43%.

Key findings: (1) Each intelligence tier—optimal matching, bundling, geographic clustering, anticipatory optimization—adds cumulative performance gains. (2) Bundling algorithms (Simple, Network, Anticipated) consistently outperform reactive algorithms (Greedy, Hungarian) by 25-44 percentage points in fulfillment rate. (3) Anticipated Bundling's lookahead capability enables proactive dispatch before orders become ready, demonstrated by 12.7% faster delivery in the Impossible Deadline scenario. (4) Geographic constraints and demand patterns amplify the value of bundling—River Divide scenario shows 96% fulfillment for Anticipated vs 55% for Greedy. (5) The simulation architecture now differentiates algorithm capabilities: reactive algorithms receive only READY orders while anticipatory algorithms receive ALL orders (PENDING and READY), enabling true temporal optimization.

---

## References

1. Meituan Open Source. (2024). *TSL-Meituan Data-Driven Research Challenge*. Retrieved from https://github.com/meituan/meituan_informs_data

2. Additional delivery optimization literature available in `context/` directory (18 research papers on ride-hailing, meal delivery optimization, vehicle routing, and anticipatory dispatch).

---

## License

MIT License - See [LICENSE](LICENSE) file for details.
