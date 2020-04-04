package com.fanxuankai.canal.mq;

import com.fanxuankai.canal.config.CanalConfig;
import com.fanxuankai.canal.constants.CommonConstants;
import com.fanxuankai.canal.constants.EventTypeConstants;
import javassist.*;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.*;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.asm.Opcodes;
import org.springframework.stereotype.Service;

import java.io.FileOutputStream;

/**
 * @author fanxuankai
 */
public class MqConsumerBeanGenerator extends ClassLoader implements Opcodes {

    public static void main(String[] args) {
        generateMqConsumer(CanalConfig.class, "t_user");
    }

    public static Class<?> generateMqConsumer(Class<?> type, String topic) {
        try {
            ClassPool pool = ClassPool.getDefault();

            CtClass clazz = pool.makeClass(type.getName() + "MqConsumer");
            ClassFile classFile = clazz.getClassFile();
            ConstPool constPool = classFile.getConstPool();
            AnnotationsAttribute classAttribute = new AnnotationsAttribute(constPool,
                    AnnotationsAttribute.visibleTag);
            // 类注解
            classAttribute.addAnnotation(new Annotation(Service.class.getName(), constPool));
            classFile.addAttribute(classAttribute);

            CtClass mqConsumerCtClass = pool.get(MqConsumer.class.getName());

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
            String updateSrcFormat = "public void update(String s) {" +
                    "org.apache.commons.lang3.tuple.Pair p = com.fanxuankai.canal.util.DomainConverter.pairOf($1, " +
                    "%s.class);" +
                    "this.mqConsumer.update(p.getLeft(), p.getRight());" +
                    "}";
            CtMethod updateMethod = CtMethod.make(String.format(updateSrcFormat, type.getName()), clazz);
            Annotation rlAnnotation = new Annotation(RabbitListener.class.getName(),
                    constPool);
            Annotation queueAnnotation = new Annotation(Queue.class.getName(), constPool);
            queueAnnotation.addMemberValue("value",
                    new StringMemberValue(topic + CommonConstants.SEPARATOR + EventTypeConstants.UPDATE, constPool));
            ArrayMemberValue arrayMemberValue = new ArrayMemberValue(constPool);
            arrayMemberValue.setValue(new MemberValue[]{new AnnotationMemberValue(queueAnnotation, constPool)});
            rlAnnotation.addMemberValue("queuesToDeclare", arrayMemberValue);
            //方法附上注解
            AnnotationsAttribute updateMethodAttr = new AnnotationsAttribute(constPool,
                    AnnotationsAttribute.visibleTag);
            updateMethodAttr.addAnnotation(rlAnnotation);
            updateMethod.getMethodInfo().addAttribute(updateMethodAttr);
            clazz.addMethod(updateMethod);
            return clazz.toClass();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void w(byte[] code, String name) throws Exception {
        //将二进制流写到本地磁盘上
        FileOutputStream fos = new FileOutputStream(name + ".class");
        fos.write(code);
        fos.close();
    }
}
