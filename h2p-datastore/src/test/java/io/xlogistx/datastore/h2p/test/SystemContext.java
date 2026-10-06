package io.xlogistx.datastore.h2p.test;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import java.lang.reflect.Method;

/**
 * Runs every test and lifecycle method of a class in the system context of
 * {@link TestSecurityController}. For the suites that exercise the store's mechanics (storage
 * model, dialects, files, dump/restore, transactions) rather than its access control: their stores
 * are opened like every store — controller + key maker, see {@link CryptoTestSupport#secure} — so
 * the access check is on, and these tests work with nobody logged in. A thread a test starts itself
 * is not covered; it enters the context with {@link TestSecurityController#systemRun}.
 */
public final class SystemContext implements InvocationInterceptor {

    private static <T> T inSystem(Invocation<T> invocation) throws Throwable {
        Throwable[] failure = new Throwable[1];
        T ret = TestSecurityController.system(() -> {
            try {
                return invocation.proceed();
            } catch (Throwable t) {
                failure[0] = t;
                return null;
            }
        });
        if (failure[0] != null) {
            throw failure[0];
        }
        return ret;
    }

    @Override
    public void interceptBeforeAllMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> ctx, ExtensionContext ext) throws Throwable {
        inSystem(invocation);
    }

    @Override
    public void interceptBeforeEachMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> ctx, ExtensionContext ext) throws Throwable {
        inSystem(invocation);
    }

    @Override
    public void interceptTestMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> ctx, ExtensionContext ext) throws Throwable {
        inSystem(invocation);
    }

    @Override
    public void interceptTestTemplateMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> ctx, ExtensionContext ext) throws Throwable {
        inSystem(invocation);
    }

    @Override
    public void interceptAfterEachMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> ctx, ExtensionContext ext) throws Throwable {
        inSystem(invocation);
    }

    @Override
    public void interceptAfterAllMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> ctx, ExtensionContext ext) throws Throwable {
        inSystem(invocation);
    }
}
