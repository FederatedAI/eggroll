
package com.webank.eggroll.clustermanager.statemechine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class EventTransitions<S, E, C> {
    private HashMap<E, List<Transition<S, E, C>>> eventTransitions = new HashMap();

    public EventTransitions() {
    }

    public void put(E event, Transition<S, E, C> transition) {
        if (this.eventTransitions.get(event) == null) {
            List<Transition<S, E, C>> transitions = new ArrayList();
            transitions.add(transition);
            this.eventTransitions.put(event, transitions);
        } else {
            List existingTransitions = (List)this.eventTransitions.get(event);
            this.verify(existingTransitions, transition);
            existingTransitions.add(transition);
        }

    }

    private void verify(List<Transition<S, E, C>> existingTransitions, Transition<S, E, C> newTransition) {
        Iterator var3 = existingTransitions.iterator();

        Transition transition;
        do {
            if (!var3.hasNext()) {
                return;
            }

            transition = (Transition)var3.next();
        } while(!transition.equals(newTransition));

        throw new RuntimeException(transition + " already Exist, you can not add another one");
    }

    public List<Transition<S, E, C>> get(E event) {
        return (List)this.eventTransitions.get(event);
    }

    public List<Transition<S, E, C>> allTransitions() {
        List<Transition<S, E, C>> allTransitions = new ArrayList();
        Iterator var2 = this.eventTransitions.values().iterator();

        while(var2.hasNext()) {
            List<Transition<S, E, C>> transitions = (List)var2.next();
            allTransitions.addAll(transitions);
        }

        return allTransitions;
    }
}
