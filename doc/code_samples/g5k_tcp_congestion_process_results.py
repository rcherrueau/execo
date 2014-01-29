import yaml, sys
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
    for tcp_congestion_control in arranged_results.keys():
        plt.plot(
            sorted(arranged_results[tcp_congestion_control].keys()),
            [ stats.scoreatpercentile(arranged_results[tcp_congestion_control][num_flows], 50)
              for num_flows in sorted(arranged_results[tcp_congestion_control].keys())],
            label = tcp_congestion_control, linewidth=0.5)
    plt.xlabel('num flows')
    plt.ylabel('bandwith bits/s')
    plt.legend()
    plt.show()
