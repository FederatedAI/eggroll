
package com.webank.eggroll.clustermanager.statemechine;


public class TransitionImpl<S, E, C> implements Transition<S, E, C> {
    private State<S, E, C> source;
    private State<S, E, C> target;
    private E event;
    private Condition<C> condition;
    private Action<S, E, C> action;
    private TransitionType type;

    public TransitionImpl() {
        this.type = TransitionType.EXTERNAL;
    }

    public State<S, E, C> getSource() {
        return this.source;
    }

    public void setSource(State<S, E, C> state) {
        this.source = state;
    }

    public E getEvent() {
        return this.event;
    }

    public void setEvent(E event) {
        this.event = event;
    }

    public void setType(TransitionType type) {
        this.type = type;
    }

    public State<S, E, C> getTarget() {
        return this.target;
    }

    public void setTarget(State<S, E, C> target) {
        this.target = target;
    }

    public Condition<C> getCondition() {
        return this.condition;
    }

    public void setCondition(Condition<C> condition) {
        this.condition = condition;
    }

    public Action<S, E, C> getAction() {
        return this.action;
    }

    public void setAction(Action<S, E, C> action) {
        this.action = action;
    }

    public State<S, E, C> transit(C ctx, boolean checkCondition) {

        this.verify();
        if (checkCondition && this.condition != null && !this.condition.isSatisfied(ctx)) {
         //   Debugger.debug("Condition is not satisfied, stay at the " + this.source + " state ");
            return this.source;
        } else {
            if (this.action != null) {
                this.action.execute(this.source.getId(), this.target.getId(), this.event, ctx);
            }

            return this.target;
        }
    }

    public final String toString() {
        return this.source + "-[" + this.event.toString() + ", " + this.type + "]->" + this.target;
    }

    public boolean equals(Object anObject) {
        if (anObject instanceof Transition) {
            Transition other = (Transition)anObject;
            if (this.event.equals(other.getEvent()) && this.source.equals(other.getSource()) && this.target.equals(other.getTarget())) {
                return true;
            }
        }

        return false;
    }

    public void verify() {
        if (this.type == TransitionType.INTERNAL && this.source != this.target) {
            throw new RuntimeException(String.format("Internal transition source state '%s' and target state '%s' must be same.", this.source, this.target));
        }
    }
}
