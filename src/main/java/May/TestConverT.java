package May;

@FunctionalInterface
public interface TestConverT<T,F> {
     F  convert(T t);
}
