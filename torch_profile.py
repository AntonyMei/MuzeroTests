import torch


def profile(func):
    from line_profiler import LineProfiler

    def wrapper(*args, **kwargs):
        lp = LineProfiler()
        lp_wrapper = lp(func)
        result = lp_wrapper(*args, **kwargs)
        lp.print_stats()
        return result
    return wrapper


@profile
def perf_test(logits, scalar_support):
    print(f"logits shape: {logits.shape}")
    print(f"scaler range: {scalar_support}")
    value_probs = torch.softmax(logits, dim=1)
    print(f"{value_probs.shape}")
    value_support = torch.ones(value_probs.shape)
    value_support[:, :] = torch.from_numpy(np.array([x for x in scalar_support]))


def main():
    logits = torch.ones([256, 601])
    scalar_support = []
    for i in range(601):
        scalar_support.append(i)
    perf_test(logits, scalar_support)


if __name__ == '__main__':
    main()