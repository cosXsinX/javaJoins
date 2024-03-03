package joins;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class Extensions {

    public static <TL, TR, TK, TJ> List<TJ> innerJoin(List<TL> lefts, List<TR> rights,
                                                      Function<TL, TK> onLeftKey, Function<TR, TK> onRightKey,
                                                      BiFunction<TL, TR, TJ> joinBuilder) {
        Map<TK, List<TR>> rightGroupsByKey = rights.stream()
                .collect(Collectors.groupingBy(onRightKey));

        return lefts.stream()
                .flatMap(left -> {
                    TK key = onLeftKey.apply(left);
                    List<TR> matchedRights = rightGroupsByKey.getOrDefault(key, Collections.emptyList());
                    return matchedRights.stream().map(right -> joinBuilder.apply(left, right));
                })
                .collect(Collectors.toList());
    }

    public static <TL, TR, TK> List<Pair<TL, TR>> leftJoin(List<TL> lefts, List<TR> rights,
                                                           Function<TL, TK> onLeftKey, Function<TR, TK> onRightKey) {
        return leftJoin(lefts, rights, onLeftKey, onRightKey, Pair::new, null);
    }

    public static <TL, TR, TK> List<Pair<TL, TR>> leftJoin(List<TL> lefts, List<TR> rights,
                                                           Function<TL, TK> onLeftKey, Function<TR, TK> onRightKey,
                                                           TR defaultJoined) {
        return leftJoin(lefts, rights, onLeftKey, onRightKey, Pair::new, defaultJoined);
    }

    public static <TL, TR, TK, TJ> List<TJ> leftJoin(List<TL> lefts, List<TR> rights,
                                                     Function<TL, TK> onLeftKey, Function<TR, TK> onRightKey,
                                                     BiFunction<TL, TR, TJ> joinBuilder, TR defaultJoined) {
        Map<TK, List<TR>> rightGroupsByKey = rights.stream()
                .collect(Collectors.groupingBy(onRightKey));
        return lefts.stream()
                .flatMap(left -> {
                    TK key = onLeftKey.apply(left);
                    List<TR> matchedRights = rightGroupsByKey.getOrDefault(key, Collections.singletonList(defaultJoined));
                    return matchedRights.stream().map(right -> joinBuilder.apply(left, right));
                })
                .collect(Collectors.toList());
    }

    public static <TL, TR, TK> List<Pair<TL, TR>> fullJoin(List<TL> lefts, List<TR> rights,
                                                           Function<TL, TK> leftKeyFunc, Function<TR, TK> rightKeyFunc,
                                                           TL defaultLeft, TR defaultRight) {
        List<Pair<TL, TR>> leftJoinResult = leftJoin(lefts, rights, leftKeyFunc, rightKeyFunc, defaultRight);
        Set<TK> joinedRightKeys = leftJoinResult.stream()
                .filter(pair -> pair.getRight() != defaultRight)
                .map(pair -> rightKeyFunc.apply(pair.getRight()))
                .collect(Collectors.toSet());

        List<Pair<TL, TR>> defaultLeftAndMissingRights = rights.stream()
                .filter(right -> !joinedRightKeys.contains(rightKeyFunc.apply(right)))
                .map(right -> new Pair<>(defaultLeft, right))
                .toList();

        List<Pair<TL, TR>> fullJoinResult = new ArrayList<>(leftJoinResult);
        fullJoinResult.addAll(defaultLeftAndMissingRights);

        return fullJoinResult;
    }
}

