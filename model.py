import argparse
import bisect
import dataclasses
import json
import logging
import math
import sys
from dataclasses import dataclass
from typing import Any, Callable, Dict, Tuple

import autograd.numpy as np
import pandas as pd
from scipy import optimize
from scipy.ndimage.interpolation import shift

import codec
import train

# Whoriz is the data "horizon" window -- i.e., the amount of past data we
# consider at every data point. It is assumed that actions past the window
# have no effect on glucose movement.
# Our periods are 5 minutes, so 12*6 = 6 hours.
Whoriz = 12 * 6
Wtime = np.linspace(0.0, 1.0 * Whoriz, Whoriz, endpoint=False, dtype="float32")
Period = "5min"


@dataclass
class Model:
    params: Dict[str, list]

    raw_insulin_sensitivities: np.ndarray
    raw_carb_ratios: np.ndarray
    raw_basals: np.ndarray

    training_loss: float


def resample(frame):
    first, last = None, None

    for col in frame.timeseries:
        index = col.series.index
        if first is None:
            first = index[0]
            last = index[len(index) - 1]
            continue

        if index[0] < first:
            first = index[0]
        if index[len(index) - 1] > last:
            last = index[len(index) - 1]

    print('FIRST', 'LAST', first, last)
    pad = pd.Series(0, pd.DatetimeIndex([first, last]))

    timeseries = []
    for i, col in enumerate(frame.timeseries):
        if col.ctype == "glucose":
            # Glucose is a "level" column, so we fill in missing
            # values with NaNs.
            resampled = col.series.resample(Period).first()
        else:
            # Carb and insulin deliveries are additive, and we pad the series
            # so that they are defined for the full timespan of the frame.
            series = col.series.combine_first(pad)
            resampled = series.resample(Period)
            resampled = resampled.sum()

        timeseries.append(codec.Timeseries(col.ctype, col.meta, resampled))

    return dataclasses.replace(frame, timeseries=timeseries)


def expia1(t, delay, tp, td):
    """Exponential insulin curve, parameterized by peak and duration,
    due to Dragan Maksimovic (@dm61).
    Worked by Pete (@ps2) in the notebook
        https://github.com/ps2/LoopIOB/blob/master/ScalableExp.ipynb
    Reworked here to be vectorized by numpy.
    """
    t = shift(t, delay)
    tau = tp * (1 - tp / td) / (1 - 2 * tp / td)
    a = 2 * (tau / td)
    S = 1 / (1 - a + (1 + a) * np.exp(-td / tau))
    return np.maximum(0.0, (S / np.power(tau, 2)) * t * (1 - t / td) * np.exp(-t / tau))


def apply_insulin_curve(column):
    assert column.ctype in ["insulin", "basal", "bolus"]

    coeffs = expia1(
        Wtime,
        column.meta["delay"] / 5,
        column.meta["peak"] / 5,
        column.meta["duration"] / 5,
    )
    # We flip them because we're going to be applying these to
    # "trailing" data.
    coeffs = np.flip(coeffs, 0)

    # ia indicates insulin activity. This is computed by adding up
    # the contributions of each delivery over a rolling window.
    # Conveniently, this is equivalent to taking the dot product of
    # the deliveries in the window with the coefficients computed
    # above.
    return column.series.rolling(window=Whoriz).apply(
        lambda pids: np.dot(pids, coeffs), raw=True
    )


def walshca(t, tdel, tdur):
    """Walsh carb absorption curves with provided delays and duration."""
    return ((t >= tdel) & (t <= tdel + tdur / 2)) * (
        4 * (t - tdel) / np.square(tdur)
    ) + ((t > tdel + tdur / 2) & (t <= tdel + tdur)) * (
        4 / tdur * (1 - (t - tdel) / tdur)
    )


def dm61_nonlinear(t, tdel, tdur, percent_end_of_rise=0.15, percent_start_of_fall=0.5):
    scale = 2. / (1. + percent_start_of_fall - percent_end_of_rise)
    percent_time = t/tdur

    def percent_absorbed(t):
        if t < 0.:
            return 0.
        if t < percent_end_of_rise:
            return 0.5*scale*math.pow(t, 2.0) / percent_end_of_rise
        if t >= percent_end_of_rise and t < percent_start_of_fall:
            return scale * (t - 0.5*percent_end_of_rise)
        if t >= percent_start_of_fall and t < 1.0:
            return scale * (percent_start_of_fall - 0.5 * percent_end_of_rise +
                            (t - percent_start_of_fall) *
                            (1.0 - 0.5 * (t - percent_start_of_fall) / (1.0 - percent_start_of_fall)))
        return 1.0

    absorbed = np.array([percent_absorbed(t) for t in percent_time])
    shifted = np.zeros_like(absorbed)
    shifted[1:] = absorbed[:len(absorbed)-1]
    coeffs = absorbed - shifted
    return coeffs


carb_curve = dm61_nonlinear


def apply_carb_curve(column):
    assert column.ctype == "carb"

    coeffs = carb_curve(
        Wtime, column.meta["delay"] / 5, column.meta["duration"] / 5)
    coeffs = np.flip(coeffs, 0)

    return column.series.rolling(window=Whoriz).apply(
        lambda ucis: np.dot(ucis, coeffs), raw=True
    )


def make_pandas_frame(frame):
    # Iterate through the columns, transforming them,
    # return a combined data frame.

    empty_index = pd.to_datetime([], unit="s", utc=True)
    empty_index = empty_index.tz_convert(frame.timezone)
    empty = pd.Series([], index=empty_index)

    insulin, carb, glucose = empty, empty, empty

    for series in frame.timeseries:
        if series.ctype in ["insulin", "basal", "bolus"]:
            insulin = insulin.add(apply_insulin_curve(
                series) / 1000.0, fill_value=0)
        elif series.ctype == "carb":
            carb = carb.add(apply_carb_curve(series), fill_value=0)
        elif series.ctype == "glucose":
            glucose = glucose.combine_first(series.series)
        else:
            raise Exception(f"unknown column type {coseriesl.ctype}")

    return pd.DataFrame({"insulin": insulin, "carb": carb, "glucose": glucose, })


default_hyper_params = {
    # These are from a hyperparameter run 2019-12-06.
    "maxdelta": 5.0,
    "maxdelta_replace": math.nan,
    "delta_window": 1,
    # Rolling window of all inputs.
    "rolling_window": 8,
    "frame_limit": None,
    "quantile_loss_quantile": 0.5,
    "optimizer": "scipy.minimize",
}


def make_frame(request, hyper_params=default_hyper_params):
    frame = resample(request)
    frame = make_pandas_frame(frame)

    frame["delta"] = frame["glucose"] - frame["glucose"].shift(1)
    maxdelta = hyper_params.get("maxdelta", 10)
    delta = frame["delta"]
    frame.loc[(delta > maxdelta) | (delta < -maxdelta), "delta"] = hyper_params.get(
        "maxdelta_replace", math.nan
    )

    if hyper_params.get("delta_window", 1) > 0:
        frame["delta"] = (
            frame["delta"]
            .rolling(window=hyper_params["delta_window"], min_periods=1)
            .mean()
        )

    win = hyper_params.get("rolling_window")
    if win > 1:
        # Compute endpoint deltas directly so we have more data points to
        # work with (fewer will get filtered out by the NaN filter).

        # frame['delta'] = (frame['sgv'] - frame['sgv'].shift(win)) / (win+1)
        # frame['ca'] = frame['ca'].rolling(window=win).mean()
        # frame['ia'] = frame['ia'].rolling(window=win).mean()

        frame = frame.rolling(window=win).mean()

    rows = (
        np.isfinite(frame["delta"])
        & np.isfinite(frame["carb"])
        & np.isfinite(frame["insulin"])
    )
    frame = frame[rows]

    # Filter carb and insulin outliers.
    frame = frame[(frame["insulin"] > 0) & (frame["insulin"] <= frame["insulin"].quantile(0.90))]
#        (frame["insulin"] >= frame["insulin"].quantile(0.05))
#        & (frame["insulin"] <= frame["insulin"].quantile(0.85))
#    ]
    carb_quantile = frame[frame["carb"] > 0.0]["carb"].quantile(0.90)
    frame = frame[frame["carb"] <= carb_quantile]

    return frame


# TODO: don't force schedules to have a parameter at index 0.
def pack_params(indexed_params, nperiod=288):
    [indices, params] = list(zip(*indexed_params))
    params = np.concatenate(params)
    indexers = []
    for index in indices:
        indexer = np.zeros(nperiod, dtype=int)
        prev = 0
        for i in range(0, len(index)):
            beg = index[i]
            if i < len(index) - 1:
                end = index[i + 1]
            else:
                end = nperiod
            indexer[beg:end] = i
        indexers.append(indexer)

    def unpack(params):
        unpacked = []
        current = 0
        for index in indices:
            n = len(index)
            unpacked.append(params[current: current + n])
            current += n
        return unpacked

    return params, indexers, unpack


def index_to_intervals(index, nperiod=288):
    if len(index) == 1:
        return [[(0, nperiod)]]
    intervals = []
    for i in range(len(index) - 1):
        intervals.append([(index[i], index[i + 1])])
    last = [(index[len(index) - 1], nperiod)]
    if index[0] > 0:
        last.append((0, index[0]))
    intervals.append(last)
    return intervals


def identify_curve(curve, index, target, nperiod=288):
    """Identity nperiod non-negative parameters that optimize the target
    applied to curves."""
    assert curve.shape == (nperiod,), f"bad curve shape {curve.shape}"
    assert target.shape == (nperiod,), f"bad target shape {target.shape}"
    assert np.shape(index)[0] <= nperiod, f"bad index shape {index.shape}"

    # First, create a [nperiod, nperiod] matrix X that computes the
    # curve for each period. We then reduce this to a [nperiod,
    # len(index)] matrix that sums contributions over each each
    # window (i.e., groups of columns) defined by the provided index.

    roll = np.zeros([nperiod, nperiod], dtype=int)
    curve_index = np.arange(nperiod)
    for i in range(roll.shape[0]):
        # Shape the curve at period i.
        roll[i, :] = np.roll(curve_index, i)

    X_rolled = curve[roll]
    X = np.zeros([nperiod, len(index)])
    for i, ivs in enumerate(index_to_intervals(index, nperiod=nperiod)):
        for beg, end in ivs:
            X[:, i] += np.sum(X_rolled[:, beg:end], axis=1)

    y = target
    x, rnorm = optimize.nnls(X, y)
    return x


def attribute_parameters(curve, index, values, nparam=24, nperiod=288):
    assert curve.shape == (nperiod,), f"bad curve shape {curve.shape}"

    roll = np.zeros([nperiod, nperiod], dtype=int)
    curve_index = np.flip(np.arange(nperiod))
    for i in range(roll.shape[0]):
        # Shape the curve at period i.
        roll[i, :] = np.roll(curve_index, i+1)
    X = curve[roll]

    for i, ivs in enumerate(index_to_intervals(index, nperiod=nperiod)):
        for beg, end in ivs:
            X[:, beg:end] *= values[i]  # np.sum(X_rolled[:, beg:end], axis=1)
    x = np.sum(X, axis=1)
    x = np.mean(np.reshape(x, (nparam, nperiod//nparam)), axis=1)
    return x


def fit(request, hyper_params=default_hyper_params, nperiod=288):
    passed_hyper_params = hyper_params
    hyper_params = {}
    hyper_params.update(passed_hyper_params)
    hyper_params.update(request.hyper_params)

    logging.info(f"fitting model with hyper parameters {hyper_params}")

    frame = make_frame(request, hyper_params=hyper_params)

    basal_insulin_curve = expia1(
        np.arange(nperiod),
        request.basal_insulin_parameters.get("delay", 5.0) / 5.0,
        request.basal_insulin_parameters["peak"] / 5.0,
        request.basal_insulin_parameters["duration"] / 5.0,
    )
    # TODO: make this the average carb curve
    default_carb_curve = carb_curve(np.arange(nperiod), 3, 36)

    # Set up parameter schedules.
    #
    # We arrange for each of basal, insulin sensitivity, and carb ratios
    # to have 24 windows in each day.
    #
    # TODO: assign windows for carb ratios based on data density
    #
    # TODO: find a better initialization strategy when no schedules are provided
    #
    # Order is: basals, insulin sensitivities, carb ratios
    if request.insulin_sensitivity_schedule is not None:
        init_insulin_sensitivity_params = attribute_parameters(
            basal_insulin_curve,
            request.insulin_sensitivity_schedule.index,
            request.insulin_sensitivity_schedule.values)
    else:
        init_insulin_sensitivity_params = 140*np.ones(24)

    if request.carb_ratio_schedule is not None:
        init_carb_ratio_params = attribute_parameters(
            default_carb_curve,
            request.carb_ratio_schedule.index,
            request.carb_ratio_schedule.values)
    else:
        init_carb_ratio_params = 15.*np.ones(24)

    if request.basal_rate_schedule is not None:
        init_basal_rate_params = attribute_parameters(
            basal_insulin_curve,
            request.basal_rate_schedule.index,
            request.basal_rate_schedule.values)
    else:
        init_basal_rate_params = np.zeros(24)

    init_params = np.concatenate([
        init_basal_rate_params,
        init_insulin_sensitivity_params,
        init_carb_ratio_params])

    def unpack_params(params):
        return params[:24], params[24:48], params[48:72]

    insulin = frame["insulin"].values
    carbs = frame["carb"].values
    deltas = frame["delta"].values

    hour = frame.index.hour
    quantile = hyper_params["quantile_loss_quantile"]

    # Construct bounds based on the allowable tuning limit.
    if request.tuning_limit is not None and request.tuning_limit > 0:
        bounds = list(zip(init_params*(1-request.tuning_limit),
                          init_params*1+request.tuning_limit))
    else:
        bounds = None

    # Re-weight entries that have carbohydrate activity so that
    # the model prefers having (much) better carb parameters
    # over slightly worse-fitting sensitivity and basal parameters.
    weights = np.ones_like(deltas)
    weights[frame["carb"] > 0] = (
        np.sum(frame["carb"] == 0) / np.sum(frame["carb"] > 0)
    )

    def model(params):
        basals, insulin_sensitivities, carb_ratios = unpack_params(params)
        basal = basals[hour]
        insulin_sensitivity = insulin_sensitivities[hour]
        carb_ratio = carb_ratios[hour]
        return insulin_sensitivity * (carbs / carb_ratio - insulin + basal)

    if bounds is not None:
        lower, upper = zip(*bounds)
        lower, upper = np.array(lower), np.array(upper)
        # This is a hack to get around the fact that basals are summed
        # over multiple hours. Thus this is only an approximate bounds,
        # but it's much simpler than the alternative.
        insulin_duration_hours = request.basal_insulin_parameters["duration"] / 60.
        lower[:24] = lower[:24]/insulin_duration_hours
        upper[:24] = upper[:24]/insulin_duration_hours

    def loss(params, iter):
        preds = model(params)
        penalty = -10.0 * np.sum(np.minimum(params, 0.0))

        # Use a barrier function if bounds are provided.
        if bounds is not None:
            # HACK: simulate a "rectified" barrier function here.
            # Note also that this doesn't work for basals since they
            # are summed up.
            epsilon = 0.00001
            penalty_params = params.copy()
            penalty_params[penalty_params >=
                           upper] = upper[penalty_params > upper]-epsilon
            penalty_params[penalty_params <=
                           lower] = lower[penalty_params <= lower]+epsilon
            penalty += np.sum(np.maximum(0., -0.01 *
                                         np.log(upper-penalty_params)))
            penalty += np.sum(np.maximum(0., -0.01 *
                                         np.log(penalty_params-lower)))

        # Quantile regression: 50 pctile
        error = weights * (deltas - preds)
        return np.mean(np.maximum(quantile * error, (quantile - 1.0) * error)) + penalty

    if hyper_params["optimizer"] == "adam":
        params, training_loss = train.minimize(loss, init_params)
    elif hyper_params["optimizer"] == "scipy.minimize":
        opt = optimize.minimize(loss, init_params, args=(0,))
        params = opt.x
        training_loss = opt.fun

    # Clip the parameters here in case the loss penalties
    # above were insufficient.
    params = np.maximum(params, 0.0)
    basals, insulin_sensitivities, carb_ratios = unpack_params(params)

    # Now, infer parameter schedules based on the optimized
    # instantaneous parameters. For carbs, we use the average
    # carb curve based on data. We also use the basal insulin
    # parameters for ISF schedules.

    if request.basal_rate_schedule is None:
        # Default: hourly
        basal_rate_index = np.arange(0, 288, 12)
    else:
        basal_rate_index = request.basal_rate_schedule.reindexed(5)
    basal_rate_schedule = (
        identify_curve(basal_insulin_curve, basal_rate_index,
                       np.repeat(basals, 12)) * 12
    )

    if request.insulin_sensitivity_schedule is None:
        insulin_sensitivity_index = np.arange(0, 288, 12 * 4)
    else:
        insulin_sensitivity_index = request.insulin_sensitivity_schedule.reindexed(
            5)
    insulin_sensitivity_schedule = identify_curve(
        basal_insulin_curve, insulin_sensitivity_index, np.repeat(
            insulin_sensitivities, 12)
    )

    if request.carb_ratio_schedule is None:
        carb_ratio_index = 12 * 6 + np.arange(0, 12 * 12, 4 * 12)
    else:
        carb_ratio_index = request.carb_ratio_schedule.reindexed(5)
    carb_ratio_schedule = identify_curve(
        default_carb_curve, carb_ratio_index, np.repeat(carb_ratios, 12)
    )

    # Finally, "quantize" the basal schedule if needed.
    #
    # TODO: Currently this simply tries to match the closest
    # allowable basal rate. We should try to push this up to the
    # model (e.g., the cost function could encourage values close to
    # allowable values), or split the schedule so so that the total
    # amount delivered over the scheduled intervals is equal to the
    # modeled amount, but the rate varies within the intervals.
    #
    # TODO: Another possibility is to perform one model run
    # to fit the basals, then another with the basals "fixed" to the
    # snapped values, allowing the model to adjust the other
    # parameters accordingly.
    #
    # TODO: collapse adjacent entries with the same value.
    if request.allowed_basal_rates is not None:
        allowed = sorted(request.allowed_basal_rates)
        for (i, rate) in enumerate(basal_rate_schedule):
            j = bisect.bisect(allowed, rate)
            # TODO: perhaps be a little more generous here,
            # snapping up when values are (much) closer.
            if j == 0 and rate != allowed[0]:
                basal_rate_schedule[i] = 0.0
            elif j >= len(allowed):
                basal_rate_schedule[i] = allowed[j - 1]
            elif rate != allowed[j]:
                basal_rate_schedule[i] = allowed[j - 1]

    def make_schedule(index, schedule):
        assert len(index) == len(schedule)
        return ((5 * index).tolist(), schedule.tolist())

    return Model(
        params={
            "insulin_sensitivity_schedule": make_schedule(
                insulin_sensitivity_index, insulin_sensitivity_schedule
            ),
            "carb_ratio_schedule": make_schedule(carb_ratio_index, carb_ratio_schedule),
            "basal_rate_schedule": make_schedule(basal_rate_index, basal_rate_schedule),
        },
        raw_insulin_sensitivities=insulin_sensitivities,
        raw_carb_ratios=carb_ratios,
        raw_basals=basals,
        training_loss=training_loss,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file", type=str, nargs="?", help="file to read")
    parser.add_argument("--output", type=str, help="output to file")
    for key, value in default_hyper_params.items():
        parser.add_argument(f"--{key}", type=type(value),
                            default=value, help=f"value for hyper parameter {key}")

    logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)

    args = parser.parse_args()

    input = open(args.file)
    if input is None:
        input = sys.stdin
    payload = json.load(input)
    request = codec.Request.fromdict(payload)
    hyper_params = {}
    hyper_params.update(default_hyper_params)
    for key in hyper_params:
        flag_value = args.__dict__[key]
        if flag_value is not None:
            hyper_params[key] = flag_value

    model = fit(request, hyper_params=hyper_params)

    resp = codec.Response(
        version=1,
        timezone=request.timezone,
        insulin_sensitivity_schedule=codec.Schedule.fromtuple(
            model.params["insulin_sensitivity_schedule"]
        ),
        carb_ratio_schedule=codec.Schedule.fromtuple(
            model.params["carb_ratio_schedule"]
        ),
        basal_rate_schedule=codec.Schedule.fromtuple(
            model.params["basal_rate_schedule"]
        ),
        training_loss=model.training_loss,
    )
    output = json.dumps(resp.todict())
    if args.output is None:
        sys.stdout.write(output)
    else:
        with open(args.output) as file:
            file.write(output)


if __name__ == "__main__":
    main()
