package io.ymq.example.elasticsearch.utils;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Aspect
@Component
public class WebLogAspect {

    /**
     * logger
     */
    private static final Logger logger = LoggerFactory.getLogger(WebLogAspect.class);

    @Pointcut("execution(public * io.ymq.example.elasticsearch..*.*(..))")
    public void webLog() {
    }

    @Before("webLog()")
    public void doBefore(JoinPoint joinPoint) throws Throwable {
        System.out.println("我是前置通知，我将要执行一个方法了");

        Signature signature = joinPoint.getSignature();
        MethodSignature methodSignature = (MethodSignature) signature;
        String[] strings = methodSignature.getParameterNames();
        System.out.println(Arrays.toString(strings));

        //获取目标方法的参数信息
        Object[] obj = joinPoint.getArgs();

        for (Object argItem : obj) {
            System.out.println("---->now-->argItem:" + argItem);
//            if (argItem instanceof String) {
//                String paramVO = (String) argItem;
//            }
            System.out.println("---->after-->argItem:" + argItem);
        }


        // 接收到请求，记录请求内容
//        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
//        HttpServletRequest request = attributes.getRequest();
//        // 记录下请求内容
//        logger.info("URL : " + request.getRequestURL().toString());
//        logger.info("HTTP_METHOD : " + request.getMethod());
//        logger.info("IP : " + request.getRemoteAddr());
//        logger.info("CLASS_METHOD : " + joinPoint.getSignature().getDeclaringTypeName() + "." + joinPoint.getSignature().getName());
        // logger.info("ARGS : " + Arrays.toString(joinPoint.getArgs()));
    }

    @AfterReturning(returning = "ret", pointcut = "webLog()")
    public void doAfterReturning(Object ret) throws Throwable {
        // 处理完请求，返回内容
        logger.info("RESPONSE : " + ret);
    }
}