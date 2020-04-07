package com.fanxuankai.canal.util;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.annotation.CanalEntity;
import com.fanxuankai.canal.constants.CommonConstants;
import com.fanxuankai.canal.mq.MqConsumer;
import com.fanxuankai.canal.redis.SimpleRedisRepository;
import com.xxl.mq.client.consumer.IMqConsumer;
import com.xxl.mq.client.consumer.MqResult;
import javassist.*;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.AttributeInfo;
import javassist.bytecode.ClassFile;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.*;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.asm.Opcodes;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.io.FileOutputStream;

/**
 * Javassist Bean 简单工厂
 *
 * @author fanxuankai
 */
public class JavassistBeanGenerator extends ClassLoader implements Opcodes {

    public static void main(String[] args) throws Exception {
        generateXxlMqConsumer(CanalEntity.class, "t_user", CanalEntry.EventType.UPDATE);
        generateRabbitMqConsumer(CanalEntity.class, "t_user");
        generateRedisRepository(CanalEntity.class, String.class).getConstructor().newInstance();
    }

    public static Class<?> generateRedisRepository(Class<?> redisRepositoryInterface, Class<?> domainType) {
        try {
            ClassPool pool = ClassPool.getDefault();
            CtClass clazz = pool.makeClass(redisRepositoryInterface.getName() +
                            "JavassistProxyImpl",
                    pool.getCtClass(SimpleRedisRepository.class.getName()));
            clazz.addInterface(pool.getCtClass(redisRepositoryInterface.getName()));
            ClassFile classFile = clazz.getClassFile();
            ConstPool constPool = classFile.getConstPool();
            AnnotationsAttribute classAttribute = new AnnotationsAttribute(constPool,
                    AnnotationsAttribute.visibleTag);
            classAttribute.addAnnotation(new Annotation(Repository.class.getName(), constPool));
            classFile.addAttribute(classAttribute);

            //添加构造函数
            CtConstructor ctConstructor = new CtConstructor(new CtClass[]{}, clazz);
            //为构造函数设置函数体
            ctConstructor.setBody(String.format("{setDomainType(%s.class);}", domainType.getName()));
            //把构造函数添加到新的类中
            clazz.addConstructor(ctConstructor);

            return clazz.toClass();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Class<?> generateXxlMqConsumer(Class<?> domainType, String topic, CanalEntry.EventType eventType) {
        try {
            ClassPool pool = ClassPool.getDefault();
            CtClass clazz = pool.makeClass(domainType.getName() + "JavassistProxyXxlMqConsumer" + eventType);
            clazz.addInterface(pool.getCtClass(IMqConsumer.class.getName()));
            ClassFile classFile = clazz.getClassFile();
            ConstPool constPool = classFile.getConstPool();
            AnnotationsAttribute classAttribute = new AnnotationsAttribute(constPool,
                    AnnotationsAttribute.visibleTag);
            // 类注解
            classAttribute.addAnnotation(new Annotation(Service.class.getName(), constPool));
            Annotation mqConsumerAnnotation =
                    new Annotation(com.xxl.mq.client.consumer.annotation.MqConsumer.class.getName(),
                            constPool);
            mqConsumerAnnotation.addMemberValue("topic",
                    new StringMemberValue(topic + CommonConstants.SEPARATOR + eventType, constPool));
            classAttribute.addAnnotation(mqConsumerAnnotation);
            classFile.addAttribute(classAttribute);
            addConsumerConstructor(clazz);
            clazz.addMethod(CtMethod.make(xxlConsumeMethodSrc(domainType, eventType), clazz));
            return clazz.toClass();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Class<?> generateRabbitMqConsumer(Class<?> domainType, String topic) {
        try {
            ClassPool pool = ClassPool.getDefault();
            CtClass clazz = pool.makeClass(domainType.getName() + "JavassistProxyRabbitMqConsumer");
            ClassFile classFile = clazz.getClassFile();
            ConstPool constPool = classFile.getConstPool();
            AnnotationsAttribute classAttribute = new AnnotationsAttribute(constPool,
                    AnnotationsAttribute.visibleTag);
            // 类注解
            classAttribute.addAnnotation(new Annotation(Service.class.getName(), constPool));
            classFile.addAttribute(classAttribute);
            addConsumerConstructor(clazz);
            // insert 方法
            String insertSrcFormat = "public void insert(String s) {" +
                    "%s t = com.fanxuankai.canal.util.DomainConverter.of($1, " +
                    "%s.class);" +
                    "this.mqConsumer.insert(t);" +
                    "}";
            CtMethod insertMethod = CtMethod.make(String.format(insertSrcFormat, domainType.getName(),
                    domainType.getName()),
                    clazz);
            insertMethod.getMethodInfo().addAttribute(rabbitMqMethodAttributeInfo(constPool, topic,
                    CanalEntry.EventType.INSERT));
            clazz.addMethod(insertMethod);
            // update 方法
            String updateSrcFormat = "public void update(String s) {" +
                    "org.apache.commons.lang3.tuple.Pair p = com.fanxuankai.canal.util.DomainConverter.pairOf($1, " +
                    "%s.class);" +
                    "this.mqConsumer.update(p.getLeft(), p.getRight());" +
                    "}";
            CtMethod updateMethod = CtMethod.make(String.format(updateSrcFormat, domainType.getName()), clazz);
            updateMethod.getMethodInfo().addAttribute(rabbitMqMethodAttributeInfo(constPool, topic,
                    CanalEntry.EventType.UPDATE));
            clazz.addMethod(updateMethod);
            // delete 方法
            String deleteSrcFormat = "public void delete(String s) {" +
                    "%s t = com.fanxuankai.canal.util.DomainConverter.of($1, " +
                    "%s.class);" +
                    "this.mqConsumer.delete(t);" +
                    "}";
            CtMethod deleteMethod = CtMethod.make(String.format(deleteSrcFormat, domainType.getName(),
                    domainType.getName()),
                    clazz);
            deleteMethod.getMethodInfo().addAttribute(rabbitMqMethodAttributeInfo(constPool, topic,
                    CanalEntry.EventType.DELETE));
            clazz.addMethod(deleteMethod);
            return clazz.toClass();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String xxlConsumeMethodSrc(Class<?> type, CanalEntry.EventType eventType) {
        StringBuilder sb = new StringBuilder();
        sb.append("public ").append(MqResult.class.getName()).append(" consume(String s) {");
        if (eventType == CanalEntry.EventType.INSERT) {
            sb.append(type.getName()).append(" t ").append(" = ").append(DomainConverter.class.getName()).append(".of" +
                    "($1, ").append(type.getName()).append(".class);");
            sb.append("this.mqConsumer.insert(t);");
        } else if (eventType == CanalEntry.EventType.UPDATE) {
            sb.append(Pair.class.getName()).append(" p ").append(" = ").append(DomainConverter.class.getName()).append(".pairOf" +
                    "($1, ").append(type.getName()).append(".class);");
            sb.append("this.mqConsumer.update(p.getLeft(), p.getRight());");
        } else if (eventType == CanalEntry.EventType.DELETE) {
            sb.append(type.getName()).append(" t ").append(" = ").append(DomainConverter.class.getName()).append(".of" +
                    "($1, ").append(type.getName()).append(".class);");
            sb.append("this.mqConsumer.delete(t);");
        }
        sb.append("return ");
        sb.append(MqResult.class.getName());
        sb.append(".SUCCESS;");
        sb.append("}");
        return sb.toString();
    }

    private static void addConsumerConstructor(CtClass clazz) throws NotFoundException, CannotCompileException {
        CtClass mqConsumerCtClass = ClassPool.getDefault().get(MqConsumer.class.getName());

        // 增加字段
        CtField mqConsumerField = new CtField(mqConsumerCtClass, "mqConsumer", clazz);
        mqConsumerField.setModifiers(Modifier.PRIVATE);
        clazz.addField(mqConsumerField);

        //添加构造函数
        CtConstructor ctConstructor = new CtConstructor(new CtClass[]{mqConsumerCtClass}, clazz);
        //为构造函数设置函数体
        ctConstructor.setBody("{this.mqConsumer=$1;}");
        //把构造函数添加到新的类中
        clazz.addConstructor(ctConstructor);
    }

    private static AttributeInfo rabbitMqMethodAttributeInfo(ConstPool constPool, String topic,
                                                             CanalEntry.EventType eventType) {
        Annotation rlAnnotation = new Annotation(RabbitListener.class.getName(), constPool);
        Annotation queueAnnotation = new Annotation(Queue.class.getName(), constPool);
        queueAnnotation.addMemberValue("value",
                new StringMemberValue(topic + CommonConstants.SEPARATOR + eventType, constPool));
        ArrayMemberValue arrayMemberValue = new ArrayMemberValue(constPool);
        arrayMemberValue.setValue(new MemberValue[]{new AnnotationMemberValue(queueAnnotation, constPool)});
        rlAnnotation.addMemberValue("queuesToDeclare", arrayMemberValue);
        //方法附上注解
        AnnotationsAttribute methodAttr = new AnnotationsAttribute(constPool,
                AnnotationsAttribute.visibleTag);
        methodAttr.addAnnotation(rlAnnotation);
        return methodAttr;
    }

    private static void w(byte[] code, String name) throws Exception {
        //将二进制流写到本地磁盘上
        FileOutputStream fos = new FileOutputStream(name + ".class");
        fos.write(code);
        fos.close();
    }
}
