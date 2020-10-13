// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.planner;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.GroupByClause;
import org.apache.doris.analysis.GroupingFunctionCallExpr;
import org.apache.doris.analysis.GroupingInfo;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TRepeatNode;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used for grouping sets.
 * It will add some new rows and a column of groupingId according to grouping sets info.
 */
public class RepeatNode extends PlanNode {

    private List<Set<Integer>> repeatSlotIdList;
    private Set<Integer> allSlotId;
    private TupleDescriptor outputTupleDesc;
    private List<List<Long>> groupingList;
    private GroupingInfo groupingInfo;
    private PlanNode input;
    private GroupByClause groupByClause;

    protected RepeatNode(PlanNodeId id, PlanNode input, GroupingInfo groupingInfo, GroupByClause groupByClause) {
        super(id, input.getTupleIds(), "REPEAT_NODE");
        this.children.add(input);
        this.groupingInfo = groupingInfo;
        this.input = input;
        this.groupByClause = groupByClause;

    }

    // only for unittest
    protected RepeatNode(PlanNodeId id, PlanNode input, List<Set<SlotId>> repeatSlotIdList,
                      TupleDescriptor outputTupleDesc, List<List<Long>> groupingList) {
        super(id, input.getTupleIds(), "REPEAT_NODE");
        this.children.add(input);
        this.repeatSlotIdList = buildIdSetList(repeatSlotIdList);
        this.groupingList = groupingList;
        this.outputTupleDesc = outputTupleDesc;
        tupleIds.add(outputTupleDesc.getId());
    }

    private static List<Set<Integer>> buildIdSetList(List<Set<SlotId>> repeatSlotIdList) {
        List<Set<Integer>> slotIdList = new ArrayList<>();
        for (Set slotSet : repeatSlotIdList) {
            Set<Integer> intSet = new HashSet<>();
            for (Object slotId : slotSet) {
                intSet.add(((SlotId) slotId).asInt());
            }
            slotIdList.add(intSet);
        }

        return slotIdList;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        avgRowSize = 0;
        cardinality = 0;
        numNodes = 1;
    }

    private void findRealSlotRef(List<Expr> originExprList, List<Object> realSlotRef) throws AnalysisException {
        for (Expr originExpr : originExprList) {
            if (originExpr instanceof SlotRef || (originExpr instanceof GroupingFunctionCallExpr)) {
                realSlotRef.add(originExpr);
                continue;
            }
            if (originExpr instanceof FunctionCallExpr) {
                FunctionCallExpr fnExpr = (FunctionCallExpr) originExpr;
                if (StringUtils.equalsIgnoreCase(fnExpr.getFn().getFunctionName().toString(), "coalesce")) {
                    Set<Expr> slotSet = new HashSet<>();
                    findSlotInChildren(fnExpr, slotSet);
                    realSlotRef.add(slotSet);
                    continue;
                }
                throw new AnalysisException("function or expr is not allowed in grouping sets clause currently.");
            }
            if (originExpr instanceof CaseExpr) {
                Set<Expr> slotSet = new HashSet<>();
                findSlotInChildren(originExpr, slotSet);
                realSlotRef.add(slotSet);
            }
        }
    }

    private void findSlotInChildren(Expr expr, Set<Expr> slotSet) {
        if (expr == null) {
            return;
        }
        if (expr instanceof SlotRef) {
            slotSet.add(expr);
            return;
        }
        for (Expr child : expr.getChildren()) {
            findSlotInChildren(child, slotSet);
        }
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        Preconditions.checkState(conjuncts.isEmpty());
        groupByClause.substituteGroupingExprs(groupingInfo.getGroupingSlots(), input.getOutputSmap(),
                analyzer);

        List<Object> realSlot = new ArrayList<>();
        findRealSlotRef(groupByClause.getGroupingExprs(), realSlot);

        // build new BitSet List for tupleDesc
        Set<SlotDescriptor> slotDescSet = new HashSet<>();
        for (TupleId tupleId : input.getTupleIds()) {
            TupleDescriptor tupleDescriptor = analyzer.getDescTbl().getTupleDesc(tupleId);
            slotDescSet.addAll(tupleDescriptor.getSlots());
        }

        // build tupleDesc according to child's tupleDesc info
        outputTupleDesc = groupingInfo.getVirtualTuple();
        //set aggregate nullable
        for (Object slotObj : realSlot) {
            if (slotObj instanceof SlotRef) {
                ((SlotRef) slotObj).getDesc().setIsNullable(true);
            }
            if (slotObj instanceof Set) {
                for (Object slot : (Set)slotObj) {
                    ((SlotRef) slot).getDesc().setIsNullable(true);
                }
            }
        }
        outputTupleDesc.computeMemLayout();

        List<Set<SlotId>> groupingIdList = new ArrayList<>();
        Preconditions.checkState(realSlot.size() >= 2);
        allSlotId = new HashSet<>();
        for (BitSet bitSet : Collections.unmodifiableList(groupingInfo.getGroupingIdList())) {
            Set<SlotId> slotIdSet = new HashSet<>();
            for (SlotDescriptor slotDesc : slotDescSet) {
                SlotId slotId = slotDesc.getId();
                if (slotId == null) {
                    continue;
                }
                for (int i = 0; i < realSlot.size(); i++) {
                    Object exprObj = realSlot.get(i);
                    if (exprObj instanceof SlotRef) {
                        SlotRef slotRef = (SlotRef) exprObj;
                        if (bitSet.get(i) && slotRef.getSlotId() == slotId) {
                            slotIdSet.add(slotId);
                            break;
                        }
                    }
                    if (exprObj instanceof Set) {
                        for (Object slot : (Set)exprObj) {
                            SlotRef slotRef = (SlotRef)slot;
                            if (bitSet.get(i) && slotRef.getSlotId() == slotId) {
                                slotIdSet.add(slotId);
                                break;
                            }
                        }
                    }
                }
            }
            groupingIdList.add(slotIdSet);
        }

        this.repeatSlotIdList = buildIdSetList(groupingIdList);
        for (Set<Integer> s : this.repeatSlotIdList) {
            allSlotId.addAll(s);
        }
        this.groupingList = groupingInfo.genGroupingList(groupByClause.getGroupingExprs());
        tupleIds.add(outputTupleDesc.getId());
        for (TupleId id : tupleIds) {
            analyzer.getTupleDesc(id).setIsMaterialized(true);
        }
        computeMemLayout(analyzer);
        computeStats(analyzer);
        createDefaultSmap(analyzer);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.REPEAT_NODE;
        msg.repeat_node = new TRepeatNode(outputTupleDesc.getId().asInt(), repeatSlotIdList, groupingList.get(0),
                groupingList, allSlotId);
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).add("Repeat", repeatSlotIdList.size()).addValue(
                super.debugString()).toString();
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(detailPrefix + "repeat: repeat ");
        output.append(repeatSlotIdList.size() - 1);
        output.append(" lines ");
        output.append(repeatSlotIdList);
        output.append("\n" );
        if (CollectionUtils.isNotEmpty(outputTupleDesc.getSlots())) {
            output.append(detailPrefix + "generate: ");
            output.append(outputTupleDesc.getSlots().stream().map(slot -> "`" + slot.getColumn().getName() + "`")
                    .collect(Collectors.joining(", ")) + "\n");
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return children.get(0).getNumInstances();
    }
}
