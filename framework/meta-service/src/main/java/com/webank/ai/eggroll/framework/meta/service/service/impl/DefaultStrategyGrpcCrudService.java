/*
 * Copyright 2019 The Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webank.ai.eggroll.framework.meta.service.service.impl;

import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.core.api.grpc.server.GrpcServerWrapper;
import com.webank.ai.eggroll.core.constant.RuntimeConstants;
import com.webank.ai.eggroll.core.error.exception.CrudException;
import com.webank.ai.eggroll.core.factory.CallMetaModelFactory;
import com.webank.ai.eggroll.core.helper.ParamValidationHelper;
import com.webank.ai.eggroll.core.serdes.impl.ByteStringSerDesHelper;
import com.webank.ai.eggroll.core.utils.ErrorUtils;
import com.webank.ai.eggroll.core.utils.ToStringUtils;
import com.webank.ai.eggroll.core.utils.TypeConversionUtils;
import com.webank.ai.eggroll.framework.meta.service.dao.generated.model.Dtable;
import com.webank.ai.eggroll.framework.meta.service.dao.generated.model.Fragment;
import com.webank.ai.eggroll.framework.meta.service.dao.generated.model.Node;
import com.webank.ai.eggroll.framework.meta.service.factory.DaoServiceFactory;
import com.webank.ai.eggroll.framework.meta.service.service.CrudServerProcessor;
import com.webank.ai.eggroll.framework.meta.service.service.GrpcCrudService;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

@Service(value = "grpcCrudService")
@Scope("prototype")
public class DefaultStrategyGrpcCrudService implements GrpcCrudService {
    @Autowired
    private ByteStringSerDesHelper byteStringSerDesHelper;
    @Autowired
    private DaoServiceFactory daoServiceFactory;
    @Autowired
    private CallMetaModelFactory callMetaModelFactory;
    @Autowired
    private ToStringUtils toStringUtils;
    @Autowired
    private ErrorUtils errorUtils;
    @Autowired
    private ParamValidationHelper paramValidationHelper;
    @Autowired
    private GrpcServerWrapper grpcServerWrapper;

    private static final Logger LOGGER = LogManager.getLogger();
    private TypeConversionUtils typeConversionUtils;

    private static final String compatibleName = "com.webank.ai.fate.eggroll.meta.service.dao.generated.model";
    private static final String currentName = "com.webank.ai.eggroll.framework.meta.service.dao.generated.model";

    private GenericDaoService genericDaoService;
    private Class recordClass;

    @Override
    public void init(Class recordClass) {
        this.recordClass = recordClass;
        this.typeConversionUtils = new TypeConversionUtils();

        if (genericDaoService == null) {
            String methodName = "create" + recordClass.getSimpleName() + "DaoService";
            try {
                genericDaoService = (GenericDaoService) MethodUtils.invokeMethod(daoServiceFactory, methodName);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public GenericDaoService getGenericDaoService() {
        return genericDaoService;
    }

    @Override
    // todo: separate createIfNotExists and create
    public void create(BasicMeta.CallRequest request, StreamObserver<BasicMeta.CallResponse> responseObserver) {
        processCrudRequest(request, responseObserver, new CrudServerProcessor<Integer>() {
            @Override
            public Integer process(Object record) {
                return genericDaoService.insertSelective(record);
/*                Integer result = null;
                try {
                    result = genericDaoService.insertSelective(record);
                } catch (RuntimeException e) {
                    Dtable dtable = (Dtable) record;

                    DtableExample dtableExample = new DtableExample();
                    dtableExample.createCriteria().andNamespaceEqualTo(dtable.getNamespace())
                            .andTableNameEqualTo(dtable.getTableName())
                            .andStatusEqualTo(DtableStatus.NORMAL.name());

                    List<Dtable> dtableList = genericDaoService.selectByExample(dtableExample);
                    if (dtableList.isEmpty()) {
                        throw e;
                    } else {
                        result = dtableList.size();
                    }
                }

                return result;*/
            }

            @Override
            public boolean isValid(Integer result) {
                return result != null && result > 0;
            }

            @Override
            public Object pickResult(Object originalRecord, Object callResult) {
                return originalRecord;
            }
        });
    }

    @Override
    // todo: combine create and update and select code when have time
    public void update(BasicMeta.CallRequest request, StreamObserver<BasicMeta.CallResponse> responseObserver) {
        processCrudRequest(request, responseObserver, new CrudServerProcessor<Integer>() {
            @Override
            public Integer process(Object record) {
                return genericDaoService.updateByPrimaryKeySelective(record);
            }

            @Override
            public boolean isValid(Integer result) {
                return result != null && result > 0;
            }

            @Override
            public Object pickResult(Object originalRecord, Object callResult) {
                return originalRecord;
            }
        });
    }

    @Override
    public void createOrUpdate(BasicMeta.CallRequest request, StreamObserver<BasicMeta.CallResponse> responseObserver) {
        processCrudRequest(request, responseObserver, new CrudServerProcessor<Integer>() {
            @Override
            public Integer process(Object record) {
                int insertRowsAffected = genericDaoService.insertSelective(record);
                if (insertRowsAffected == 1) {
                    return insertRowsAffected;
                }

                int updateRowsAffected = genericDaoService.updateByPrimaryKeySelective(record);

                return updateRowsAffected;
            }

            @Override
            public boolean isValid(Integer result) {
                return result != null && result > 0;
            }

            @Override
            public Object pickResult(Object originalRecord, Object callResult) {
                return originalRecord;
            }
        });
    }

    @Override
    // todo: combine create and update and select code when have time
    public void getById(BasicMeta.CallRequest request, StreamObserver responseObserver) {
        processCrudRequest(request, responseObserver, new CrudServerProcessor<Object>() {
            @Override
            public Object process(Object record) {
                return genericDaoService.selectByPrimaryKey(record);
            }

            @Override
            public boolean isValid(Object result) {
                return true;
            }

            @Override
            public Object pickResult(Object originalRecord, Object callResult) {
                return callResult;
            }
        });
    }


/*    public <T> void processCrudRequestInternal(BasicMeta.CallRequest request,
                                               StreamObserver response,
                                               CrudServerProcessor<T> crudServerProcessor) {
        BasicMeta.CallResponse result = null;

        try {
            paramValidationHelper.validate(request);

            BasicMeta.Data requestData = request.getParam();

            Object record = byteStringSerDesHelper.deserialize(requestData.getData(), Class.forName(requestData.getType()));

            T callResult = (T) crudServerProcessor.process(record);

            if (crudServerProcessor.isValid(callResult)) {
                result = callMetaModelFactory.createNormalCallResponse(crudServerProcessor.pickResult(record, callResult));
            } else {
                result = callMetaModelFactory.createErrorCallResponse(
                        102, "Failed to perform" + recordClass.getSimpleName() + " crud operation in database", record);
            }

            response.onNext(result);
            response.onCompleted();
        } catch (Exception e) {
            response.onError(errorUtils.toGrpcRuntimeException(e));
        }
    }*/

    @Override
    public <T> void processCrudRequest(BasicMeta.CallRequest request,
                                       StreamObserver responseObserver,
                                       CrudServerProcessor<T> crudServerProcessor) {
        grpcServerWrapper.wrapGrpcServerRunnable(responseObserver, () -> {
            BasicMeta.CallResponse result = null;
            paramValidationHelper.validate(request);

            BasicMeta.Data requestData = request.getParam();

            boolean compatible = false;
            String hostLanguage = requestData.getHostLanguage();
            String paramType = requestData.getType();

            if (!hostLanguage.contains(RuntimeConstants.HOST_LANGUAGE)) {
                compatible = true;
            }

            Object record = byteStringSerDesHelper.deserialize(requestData.getData(), Class.forName(paramType));

            if (compatible) {
                if (record instanceof com.webank.ai.fate.eggroll.meta.service.dao.generated.model.Node) {
                    record = typeConversionUtils.toCurrentNode((com.webank.ai.fate.eggroll.meta.service.dao.generated.model.Node) record);
                } else if (record instanceof com.webank.ai.fate.eggroll.meta.service.dao.generated.model.Dtable) {
                    record = typeConversionUtils.toCurrentDtable((com.webank.ai.fate.eggroll.meta.service.dao.generated.model.Dtable) record);
                } else if(record instanceof com.webank.ai.fate.eggroll.meta.service.dao.generated.model.Fragment) {
                    record = typeConversionUtils.toCurrentFragment((com.webank.ai.fate.eggroll.meta.service.dao.generated.model.Fragment) record);
                }
            }
            Object callResult = processCrudRequest(record, crudServerProcessor);

            if (!compatible) {
                result = callMetaModelFactory.createNormalCallResponse(callResult);
            } else {
                if (callResult == null) {
                    result = callMetaModelFactory.createNormalCallResponse(callResult);
                } else {
                    if (callResult instanceof List) {
                        List listedCallResult = (List) callResult;
                        Object first = listedCallResult.isEmpty() ? null : listedCallResult.get(0);

                        if (first instanceof Node) {
                            ArrayList<Node> arrayCurrentResult = (ArrayList<Node>) callResult;
                            ArrayList<com.webank.ai.fate.eggroll.meta.service.dao.generated.model.Node> arrayCompatibleResult = new ArrayList<>();
                            for (Node node : arrayCurrentResult) {
                                arrayCompatibleResult.add(typeConversionUtils.toCompatibleNode(node));
                            }

                            result = callMetaModelFactory.createNormalCallResponse(arrayCompatibleResult);
                        } else if (first instanceof Dtable) {
                            if (callResult instanceof List) {
                                ArrayList<Dtable> arrayCurrentResult = (ArrayList<Dtable>) callResult;
                                ArrayList<com.webank.ai.fate.eggroll.meta.service.dao.generated.model.Dtable> arrayCompatibleResult = new ArrayList<>();
                                for (Dtable dtable : arrayCurrentResult) {
                                    arrayCompatibleResult.add(typeConversionUtils.toCompatibleDtable(dtable));
                                }

                                result = callMetaModelFactory.createNormalCallResponse(arrayCompatibleResult);
                            }
                        } else if (first instanceof Fragment) {
                            if (callResult instanceof List) {
                                ArrayList<Fragment> arrayCurrentResult = (ArrayList<Fragment>) callResult;
                                ArrayList<com.webank.ai.fate.eggroll.meta.service.dao.generated.model.Fragment> arrayCompatibleResult = new ArrayList<>();
                                for (Fragment fragment : arrayCurrentResult) {
                                    arrayCompatibleResult.add(typeConversionUtils.toCompatibleFragment(fragment));
                                }

                                result = callMetaModelFactory.createNormalCallResponse(arrayCompatibleResult);
                            }
                        }
                    } else {
                        if (callResult instanceof Node) {
                            result = callMetaModelFactory.createNormalCallResponse(typeConversionUtils.toCompatibleNode((Node) callResult));
                        } else if (callResult instanceof Dtable) {
                            result = callMetaModelFactory.createNormalCallResponse(typeConversionUtils.toCompatibleDtable((Dtable) callResult));
                        } else if (callResult instanceof Fragment) {
                            result = callMetaModelFactory.createNormalCallResponse(typeConversionUtils.toCompatibleFragment((Fragment) callResult));
                        }
                    }
                }
            }

            responseObserver.onNext(result);
            responseObserver.onCompleted();
        });
    }

    @Override
    public <T> Object processCrudRequest(Object record, CrudServerProcessor<T> crudServerProcessor) throws CrudException {

        T callResult = (T) crudServerProcessor.process(record);

        if (crudServerProcessor.isValid(callResult)) {
            return crudServerProcessor.pickResult(record, callResult);
        } else {
            throw new CrudException(102, "Failed to perform " + recordClass.getSimpleName() + " crud operation in database");
        }
    }

}
