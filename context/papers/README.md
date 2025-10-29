# Delivery Pricing & Optimization Research

## Downloaded Academic Papers

### 1. Sullivan (2024) - Price Controls in Multi-Sided Markets
**File**: `sullivan-2024-price-controls-food-delivery.pdf` (4.5 MB)
**URL**: https://m-r-sullivan.github.io/assets/papers/food_delivery_cap.pdf
**Summary**: Evaluates caps on commissions that food delivery platforms charge restaurants using maximum likelihood estimation of consumer preferences and GMM estimation of restaurant platform adoption models.

### 2. arXiv (2025) - Pricing with Tips in Three-Sided Delivery Platforms
**File**: `pricing-with-tips-three-sided-delivery-2025.pdf` (630 KB)
**URL**: https://arxiv.org/abs/2507.10872
**Summary**: Illustrates the role of tips in pricing equilibrium, showing that without tips, equilibrium is only guaranteed when there are at least as many couriers as customers.

### 3. Uber (2024) - Practical Marketplace Optimization Using Causally-Informed ML
**File**: `uber-practical-marketplace-optimization-2024.pdf` (1.0 MB)
**URL**: https://arxiv.org/pdf/2407.19078.pdf
**Summary**: End-to-end machine learning and optimization procedure to automate budget decision-making for cities where Uber operates, combining causal inference with optimization.

### 4. UC Davis Dissertation - RL-based Pricing (FAILED DOWNLOAD)
**File**: `uc-davis-rl-food-delivery-pricing.pdf` (0 bytes - needs re-download)
**URL**: https://escholarship.org/content/qt07h3d59m/qt07h3d59m.pdf
**Summary**: Formulates and solves dynamic, zone-based pricing for online food delivery services using reinforcement learning in San Francisco.

---

## Papers Still Needed

### Priority Downloads:

1. **SSRN - Operating Three-Sided Marketplace** (Liu, Shen, Sun 2023)
   - URL: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4668867
   - Note: 80-page paper, most comprehensive for delivery pricing
   - Topics: State-dependent Markovian queueing, heavy traffic analysis, spatial staffing

2. **ScienceDirect - Three-Sided Market of On-Demand Delivery**
   - URL: https://www.sciencedirect.com/science/article/abs/pii/S1366554523003010
   - Note: Requires institutional access or purchase
   - Topics: Pricing strategies for platform equilibrium manipulation

3. **Frontiers - Delivery Fee Pricing Factors** (2022)
   - URL: https://www.frontiersin.org/journals/future-transportation/articles/10.3389/ffutr.2022.1031021/full
   - Topics: Distance, waiting time, order day, product type effects on delivery fees
   - Data: Brazilian platforms

4. **ScienceDirect - Service Pricing Strategy of Food Delivery Platform**
   - URL: https://www.sciencedirect.com/science/article/abs/pii/S2210539522001250
   - Topics: Quality differentiation models in multi-sided markets

---

## Uber Engineering Blog Posts (To be saved as PDF)

### Food Delivery Focused:

1. **Food Discovery with Uber Eats** (2018)
   - URL: https://www.uber.com/blog/uber-eats-recommending-marketplace/
   - Topics: Three-sided marketplace, multi-objective optimization, quadratic programming

2. **Reinforcement Learning for Marketplace Balance** (2025)
   - URL: https://www.uber.com/blog/reinforcement-learning-for-modeling-marketplace-balance/
   - Topics: Deep Q-learning, driver positioning, temporal difference learning

3. **Driver Surge Pricing** (2025)
   - URL: https://www.uber.com/blog/research/driver-surge-pricing/
   - Topics: Incentive-compatible pricing, Bayesian inference, personalized surge

4. **Dynamic Pricing and Matching in Ride-Hailing**
   - URL: https://www.uber.com/blog/research/dynamic-pricing-and-matching-in-ride-hailing-platforms/
   - Topics: Dynamic waiting, pool-matching, joint optimization

---

## How to Save Blog Posts as PDF

**Option 1: Browser Print to PDF**
1. Open blog post in browser
2. File → Print → Save as PDF
3. Name: `uber-{topic}-{year}.pdf`

**Option 2: Command Line (wkhtmltopdf)**
```bash
brew install wkhtmltopdf
wkhtmltopdf https://www.uber.com/blog/uber-eats-recommending-marketplace/ uber-eats-marketplace-2018.pdf
```

**Option 3: Python (pdfkit)**
```python
import pdfkit
pdfkit.from_url('https://www.uber.com/blog/...', 'output.pdf')
```

---

## Research Themes Across Papers

1. **Three-Sided Marketplace Dynamics**: Balancing eaters, restaurants, couriers
2. **Multi-Objective Optimization**: Conversion, fairness, diversity, utilization
3. **Reinforcement Learning**: DQN, temporal difference, value functions
4. **Queueing Theory**: Spatial staffing, service rates, heavy traffic
5. **Causal Inference**: Experimentation, measurement, optimization
6. **Dynamic Pricing**: Surge, zone-based, personalized, incentive-compatible
7. **Matching Algorithms**: Hungarian, dynamic waiting, pool-matching
