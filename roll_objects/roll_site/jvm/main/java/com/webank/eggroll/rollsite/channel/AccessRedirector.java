package com.webank.eggroll.rollsite.channel;

import io.grpc.*;

import java.util.HashMap;
import java.util.Map;

public class AccessRedirector {
    public ServerServiceDefinition redirect(ServerServiceDefinition serverServiceDefinition, String existing, String redirected) {
        ServiceDescriptor serviceDescriptor = serverServiceDefinition.getServiceDescriptor();

        ServiceDescriptor.Builder serviceDescriptorBuilder = ServiceDescriptor.newBuilder(redirected);
        Map<String, MethodDescriptor> existingToRedirectMethodDescriptor = new HashMap<>();

        for (MethodDescriptor methodDescriptor : serviceDescriptor.getMethods()) {
            String existingMethodName = methodDescriptor.getFullMethodName();
            MethodDescriptor existingDescriptor = methodDescriptor.toBuilder().setFullMethodName(existingMethodName.replaceAll(existing, redirected)).build();

            serviceDescriptorBuilder.addMethod(existingDescriptor);
            existingToRedirectMethodDescriptor.put(existingMethodName, existingDescriptor);
        }

        ServerServiceDefinition.Builder redirectedServiceDefinitionBuilder = ServerServiceDefinition.builder(serviceDescriptorBuilder.build());
        for (ServerMethodDefinition serverMethodDefinition : serverServiceDefinition.getMethods()) {
            String existingMethodName = serverMethodDefinition.getMethodDescriptor().getFullMethodName();
            redirectedServiceDefinitionBuilder.addMethod(
                    ServerMethodDefinition.create(existingToRedirectMethodDescriptor.get(existingMethodName),
                            serverMethodDefinition.getServerCallHandler()));
        }

        return redirectedServiceDefinitionBuilder.build();
    }

    public ServerServiceDefinition redirect(BindableService bindableService, String existing, String redirected) {
        return redirect(bindableService.bindService(), existing, redirected);
    }
}
