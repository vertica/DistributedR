#ifndef OBSERVER_H
#define OBSERVER_H

#include <assert.h>
#include <vector>

template<class T>
class IObserver
{
public:
    virtual ~IObserver() {}
    virtual void Update(T& data) = 0;
};


template<class T>
class ISubject
{
public:
    virtual ~ISubject(){}

    virtual void Subscribe(IObserver<T> *observer) {
        mObservers.push_back(observer);
    }

    virtual void Unsubscribe(IObserver<T> *observer) {
        assert(0); //unimplemented TODO
    }

    virtual void Notify(T& data) {
        for (auto i = mObservers.begin(); i != mObservers.end(); ++i){
            (*i)->Update(data);
        }
    }

    typedef std::vector<IObserver<T> *> ObserverList;
    ObserverList mObservers;
};

#endif // OBSERVER_H
