import yaml, sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from scipy import stats

if __name__ == "__main__":
    fname = sys.argv[1]
    with open(fname, "r") as f:
        results = yaml.load(f)
    arranged_results = {
        tcp_congestion_control:
            { num_flows:
                  [ result["bw"]
                    for result in results
                    if (result["params"]["num_flows"] == num_flows
                        and result["params"]["tcp_congestion_control"] == tcp_congestion_control) ]
              for num_flows in
              [ result["params"]["num_flows"] for result in results ] }
        for tcp_congestion_control in
        [ result["params"]["tcp_congestion_control"] for result in results ] }
    offset = -.18
    for tcp_congestion_control in arranged_results.keys():
        plt.boxplot(arranged_results[tcp_congestion_control].values(),
                    positions = [pos + offset for pos in arranged_results[tcp_congestion_control].keys() ])
        plt.plot(
            sorted(arranged_results[tcp_congestion_control].keys()),
            [ stats.scoreatpercentile(arranged_results[tcp_congestion_control][num_flows], 50)
              for num_flows in sorted(arranged_results[tcp_congestion_control].keys())],
            label = tcp_congestion_control, linewidth=0.5)
        offset += .36
    plt.xlim(0, max(arranged_results[tcp_congestion_control].keys()) + 1)
    plt.xticks(arranged_results[tcp_congestion_control].keys())
    plt.legend(loc = 'lower right')
    plt.xlabel('num flows')
    plt.ylabel('bandwith bits/s')
    plt.savefig("g5k_tcp_congestion.png")
